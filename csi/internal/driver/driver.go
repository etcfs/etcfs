// Package driver implements the EtcFS Container Storage Interface plugin.
//
// A CSI "volume" is a subdirectory of one EtcFS filesystem, not a device: the
// filesystem is already shared and already coordinated, so provisioning is a
// directory and publishing is a bind mount. Everything that makes EtcFS safe
// under concurrent writers — leases, self-fencing, external fencing,
// generation-stamped extents — stays where it is, in the daemon and the
// fencing controller. The driver's job is to give Kubernetes a handle on that
// filesystem and to route the one Kubernetes-shaped event that matters (a node
// releasing a volume it can no longer be trusted with) into the fence path
// that already exists.
package driver

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/etcfs/etcfs/pkg/metadata"
)

// DefaultName is the CSI driver name, matching the `provisioner` field of a
// StorageClass and the `driver` field of a CSIDriver object.
const DefaultName = "csi.etcfs.io"

// Mode selects which of the two CSI services this process serves.
type Mode string

const (
	// ModeController runs the provisioning and attach/detach half. It needs
	// etcd credentials and a mount of the filesystem.
	ModeController Mode = "controller"
	// ModeNode runs the per-host half: bind mounts and volume statistics. It
	// needs no etcd access.
	ModeNode Mode = "node"
	// ModeAll runs both in one process, for single-binary testing.
	ModeAll Mode = "all"
)

// Config is the driver's runtime configuration.
type Config struct {
	Name     string
	Version  string
	Mode     Mode
	Endpoint string // gRPC listen endpoint, e.g. unix:///csi/csi.sock

	// NodeID must be the same identifier the EtcFS daemon registers in
	// membership on this host. The fence path is keyed by it: a CSI node ID
	// that does not match the daemon's --node-id would look up a member that
	// does not exist and report every node as departed.
	NodeID string

	// MountPath is where the EtcFS filesystem is mounted on the host. Volumes
	// are subdirectories of it.
	MountPath string

	// etcd access, controller only.
	EtcdEndpoints []string
	EtcdCertFile  string
	EtcdKeyFile   string
	EtcdCAFile    string
}

// Driver serves the CSI services selected by its mode.
type Driver struct {
	cfg   Config
	store *metadata.Store // nil in node mode
	srv   *grpc.Server
}

// New validates the configuration and builds a driver. The metadata store is
// supplied by the caller so the controller's etcd client is owned — and closed
// — where it is created.
func New(cfg Config, store *metadata.Store) (*Driver, error) {
	if cfg.Name == "" {
		cfg.Name = DefaultName
	}
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("node ID is required")
	}
	if cfg.MountPath == "" {
		return nil, fmt.Errorf("mount path is required")
	}
	if cfg.Mode != ModeNode && store == nil {
		return nil, fmt.Errorf("mode %s requires an etcd-backed metadata store", cfg.Mode)
	}
	return &Driver{cfg: cfg, store: store}, nil
}

// Run serves gRPC until ctx is cancelled.
func (d *Driver) Run(ctx context.Context) error {
	network, addr, err := parseEndpoint(d.cfg.Endpoint)
	if err != nil {
		return err
	}
	if network == "unix" {
		// The socket is re-bound at every start; a leftover file from a killed
		// process would otherwise make bind fail with EADDRINUSE forever.
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove stale socket %s: %w", addr, err)
		}
		if err := os.MkdirAll(filepath.Dir(addr), 0o750); err != nil {
			return fmt.Errorf("create socket directory: %w", err)
		}
	}
	lis, err := net.Listen(network, addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", d.cfg.Endpoint, err)
	}

	d.srv = grpc.NewServer(grpc.UnaryInterceptor(logInterceptor))
	d.register(d.srv)

	go func() {
		<-ctx.Done()
		d.srv.GracefulStop()
	}()
	return d.srv.Serve(lis)
}

// register wires the services selected by d.cfg.Mode onto srv. Split out of
// Run so a test can register the same driver onto an in-process (bufconn)
// server instead of a real socket.
func (d *Driver) register(srv *grpc.Server) {
	csi.RegisterIdentityServer(srv, &identityServer{cfg: d.cfg})
	if d.cfg.Mode == ModeController || d.cfg.Mode == ModeAll {
		csi.RegisterControllerServer(srv, &controllerServer{cfg: d.cfg, store: d.store})
	}
	if d.cfg.Mode == ModeNode || d.cfg.Mode == ModeAll {
		csi.RegisterNodeServer(srv, &nodeServer{cfg: d.cfg})
	}
}

func parseEndpoint(endpoint string) (network, addr string, err error) {
	switch {
	case strings.HasPrefix(endpoint, "unix://"):
		return "unix", strings.TrimPrefix(endpoint, "unix://"), nil
	case strings.HasPrefix(endpoint, "tcp://"):
		return "tcp", strings.TrimPrefix(endpoint, "tcp://"), nil
	default:
		return "", "", fmt.Errorf("unsupported endpoint %q: want unix:// or tcp://", endpoint)
	}
}

// logInterceptor logs the failing calls only. A CSI plugin is polled
// constantly by its sidecars, and logging every successful Probe buries the
// one call that mattered.
func logInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	if err != nil && status.Code(err) != codes.OK {
		fmt.Fprintf(os.Stderr, "csi: %s: %v\n", info.FullMethod, err)
	}
	return resp, err
}
