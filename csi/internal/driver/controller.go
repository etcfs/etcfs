package driver

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/etcfs/etcfs/pkg/metadata"
)

type controllerServer struct {
	csi.UnimplementedControllerServer
	cfg   Config
	store *metadata.Store
}

func (s *controllerServer) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (
	*csi.ControllerGetCapabilitiesResponse, error) {
	rpc := func(t csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability {
		return &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{Type: t},
			},
		}
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: []*csi.ControllerServiceCapability{
		rpc(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME),
		rpc(csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME),
	}}, nil
}

// CreateVolume provisions a subdirectory of the shared filesystem.
//
// The controller must itself have the filesystem mounted at cfg.MountPath: the
// directory has to exist before any node publishes it, and creating it through
// the filesystem is what makes it visible to every other node at once.
func (s *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}
	if err := validateCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}
	dir, err := volumeDir(s.cfg.MountPath, req.GetName())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// MkdirAll rather than Mkdir: CreateVolume is required to be idempotent,
	// and the provisioner retries it after any ambiguous failure.
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return nil, status.Errorf(codes.Internal, "create volume directory: %v", err)
	}

	capacity := req.GetCapacityRange().GetRequiredBytes()
	if capacity > 0 {
		if err := s.setQuota(ctx, dir, uint64(capacity)); err != nil {
			return nil, err
		}
	}

	return &csi.CreateVolumeResponse{Volume: &csi.Volume{
		VolumeId:      req.GetName(),
		CapacityBytes: capacity,
		VolumeContext: req.GetParameters(),
	}}, nil
}

// setQuota records the requested size as a quota root on the volume directory.
//
// EtcFS quotas are soft — usage is computed by a sweep, not enforced inside
// the write path — so this makes a PersistentVolumeClaim's size reportable and
// alertable, not a hard ceiling. A claim that exceeds its size keeps working;
// `etcfsctl quota` and the quota metrics are where it shows up.
func (s *controllerServer) setQuota(ctx context.Context, dir string, bytes uint64) error {
	ino, err := inodeOf(dir)
	if err != nil {
		return status.Errorf(codes.Internal, "resolve volume inode: %v", err)
	}
	if err := s.store.SetQuota(ctx, ino, metadata.QuotaRecord{Bytes: bytes}); err != nil {
		return status.Errorf(codes.Internal, "set quota: %v", err)
	}
	return nil
}

func (s *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (
	*csi.DeleteVolumeResponse, error) {
	dir, err := volumeDir(s.cfg.MountPath, req.GetVolumeId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// The quota record is keyed by inode, so it has to be cleared before the
	// directory goes: afterwards the number is unrecoverable, and a reused
	// inode would inherit this volume's limit.
	if ino, err := inodeOf(dir); err == nil {
		if err := s.store.ClearQuota(ctx, ino); err != nil {
			return nil, status.Errorf(codes.Internal, "clear quota: %v", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, status.Errorf(codes.Internal, "resolve volume inode: %v", err)
	}

	if err := os.RemoveAll(dir); err != nil {
		return nil, status.Errorf(codes.Internal, "delete volume directory: %v", err)
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume records nothing and attaches nothing: the shared
// device is already attached to every node, and the filesystem is already
// mounted there. What it does do is refuse a node that is not a live member of
// the EtcFS cluster, so a pod is not scheduled onto a host whose daemon is
// down or whose lease has expired — on that host the mount path is either
// missing or an empty local directory.
func (s *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (
	*csi.ControllerPublishVolumeResponse, error) {
	if err := validateCapability(req.GetVolumeCapability()); err != nil {
		return nil, err
	}
	if _, err := volumeDir(s.cfg.MountPath, req.GetVolumeId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	alive, err := s.isLiveMember(ctx, req.GetNodeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "look up node %s: %v", req.GetNodeId(), err)
	}
	if !alive {
		return nil, status.Errorf(codes.FailedPrecondition,
			"node %s holds no EtcFS membership lease: its daemon is not running or has been fenced",
			req.GetNodeId())
	}
	return &csi.ControllerPublishVolumeResponse{}, nil
}

// ControllerUnpublishVolume is where Kubernetes' volume lifecycle meets the
// fencing protocol.
//
// It records a fence intent only for a node that no longer holds a membership
// lease. That condition is the whole design: a node releasing a volume during
// an ordinary pod rescheduling is healthy, still renewing its lease, and
// fencing it would take out a working host. A node whose lease is gone is one
// whose self-fencing watchdog has already stopped it or which etcd can no
// longer reach — the case where Kubernetes today waits for a human to apply
// the out-of-service taint.
//
// The fence itself is never executed here: pkg/fencing.Controller's
// reconciliation sweep claims the intent and completes it with dual
// confirmation, which is also why an intent recorded against a live node would
// be dropped rather than acted on.
func (s *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (
	*csi.ControllerUnpublishVolumeResponse, error) {
	nodeID := req.GetNodeId()
	if nodeID == "" {
		// An empty node ID means "unpublish from every node", and there is
		// nothing per-node to undo.
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	alive, err := s.isLiveMember(ctx, nodeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "look up node %s: %v", nodeID, err)
	}
	if alive {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	// The departed node's membership key is gone and its instance ID with it,
	// so the fence degrades to a generation bump unless the controller learns
	// the instance another way — the same trade etcfsctl fence makes.
	if err := s.store.RecordFenceIntent(ctx, nodeID, ""); err != nil {
		return nil, status.Errorf(codes.Internal, "record fence intent for %s: %v", nodeID, err)
	}
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (s *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (
	*csi.ValidateVolumeCapabilitiesResponse, error) {
	dir, err := volumeDir(s.cfg.MountPath, req.GetVolumeId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if _, err := os.Stat(dir); err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %s: %v", req.GetVolumeId(), err)
	}
	if err := validateCapabilities(req.GetVolumeCapabilities()); err != nil {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: err.Error()}, nil
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

func (s *controllerServer) isLiveMember(ctx context.Context, nodeID string) (bool, error) {
	value, err := s.store.Get(ctx, metadata.MembershipKey(nodeID))
	if err != nil {
		return false, err
	}
	return value != nil, nil
}

// validateCapabilities rejects block volumes. EtcFS publishes a directory of a
// mounted filesystem; handing a pod a raw device would mean handing it the
// shared device itself, unmediated by the coordination that makes sharing safe.
func validateCapabilities(caps []*csi.VolumeCapability) error {
	if len(caps) == 0 {
		return status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	for _, c := range caps {
		if err := validateCapability(c); err != nil {
			return err
		}
	}
	return nil
}

func validateCapability(c *csi.VolumeCapability) error {
	if c == nil {
		return status.Error(codes.InvalidArgument, "volume capability is required")
	}
	if c.GetBlock() != nil {
		return status.Error(codes.InvalidArgument,
			fmt.Sprintf("volumeMode Block is not supported by %s: EtcFS publishes a directory of a "+
				"shared filesystem, not the underlying device", DefaultName))
	}
	if c.GetMount() == nil {
		return status.Error(codes.InvalidArgument, "only mount volumes are supported")
	}
	return nil
}
