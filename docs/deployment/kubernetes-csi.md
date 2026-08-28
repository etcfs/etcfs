# Kubernetes: the EtcFS CSI Driver

The CSI driver lets Kubernetes workloads use an EtcFS filesystem as a
`ReadWriteMany` volume: many pods on many nodes writing to one shared raw
block device, with EtcFS providing the coordination and the fencing that a
shared device otherwise leaves to the application.

The driver lives in `csi/`, a nested Go module. Its gRPC and
container-storage-interface dependencies stay out of the root module, so
`etcfuse-meta`, `etcfuse` and `etcfsctl` are unaffected by it; the module in
turn depends on the root module for `pkg/metadata`, which is how it reaches
membership and the fence path.

Validated on a real 2-node EKS cluster with a real io2 Multi-Attach EBS
volume — dynamic provisioning, cross-node `ReadWriteMany` visibility, and
quota recording all confirmed working. See the
[EKS CSI Driver Validation report](../reports/csi-reports/eks-csi-driver-validation.md)
for the full run, including one real deployment bug it found and fixed (the
`etcfuse` runtime image was missing `/bin/mount`, now corrected).

## What a volume is

A CSI volume is **a subdirectory of one EtcFS filesystem**, not a device and
not a separate filesystem. The device is already attached to every node and the
filesystem is already mounted there by the EtcFS daemon; provisioning a volume
is therefore creating a directory, and publishing one is a bind mount from that
directory to the pod's target path.

That model has three consequences worth knowing before you install anything:

- **Capacity is cluster-wide.** A claim's `resources.requests.storage` becomes
  a *soft* quota on the volume's directory — visible through
  `etcfsctl quota`, not enforced inside the write path. `df` inside a pod
  reports the whole filesystem's capacity. See
  [etcfsctl](etcfsctl.md) for reading quota usage.
- **`volumeMode: Block` is rejected.** Handing a pod the raw device would hand
  it the shared device unmediated by the coordination that makes sharing safe.
- **The driver never mounts the filesystem.** If the daemon on a node is not
  running, the node plugin refuses to publish rather than bind-mounting an
  empty local directory that looks shared and is not.

## Prerequisites

1. **An EtcFS cluster already running on the Kubernetes nodes** — `etcfuse-meta`
   and `etcfuse` per node, sharing one block device, mounted at a fixed host
   path (`/mnt/etcfs` by default). See [Configuration](configuration.md).
2. **EtcFS's own etcd.** Do not point the driver at the Kubernetes control
   plane's etcd. Filesystem metadata is high-churn data-plane traffic — an
   inode record per file, an extent record per write, a lock acquisition per
   operation — and exhausting the API server's etcd takes down scheduling
   cluster-wide. On EKS, GKE and AKS the control-plane etcd is not reachable
   at all. Running EtcFS's own etcd *on* Kubernetes (a StatefulSet with its own
   quota and compaction settings) is fine.
3. **Matching node identifiers.** The driver's `--node-id` is
   `spec.nodeName`, and it must equal the `--node-id` the EtcFS daemon
   registers in membership on that host — the daemon's default is the hostname.
   If the two disagree, every node looks departed to the controller and no
   volume will publish.
4. Kubernetes 1.24 or later, and Helm 3.

Prerequisites 1 and 2 — a running EtcFS cluster with its own etcd, on nodes
the CSI driver can reach — are exactly what
`infra/terraform-eks`
provisions in one `terraform apply`, if starting from nothing rather than an
existing EKS cluster: control plane, worker nodes, a shared io2 Multi-Attach
volume, EtcFS's own etcd and daemon pair, and this chart, all in one
configuration. See that directory's `README.md`.

## Installation

```bash
helm install etcfs-csi ./csi/deploy/helm/etcfs-csi \
  --namespace kube-system \
  --set etcd.endpoints=https://etcd-0.etcfs:2379,https://etcd-1.etcfs:2379 \
  --set mountPath=/mnt/etcfs
```

With client TLS on etcd, create a secret holding `ca.crt`, `tls.crt` and
`tls.key` and reference it:

```bash
kubectl -n kube-system create secret generic etcfs-etcd-tls \
  --from-file=ca.crt=certs/ca.crt \
  --from-file=tls.crt=certs/client.crt \
  --from-file=tls.key=certs/client.key

helm install etcfs-csi ./csi/deploy/helm/etcfs-csi \
  --namespace kube-system \
  --set etcd.endpoints=https://etcd-0.etcfs:2379 \
  --set etcd.tlsSecretName=etcfs-etcd-tls
```

The chart installs a `CSIDriver` object, a node `DaemonSet`
(`node-driver-registrar` plus the plugin, privileged, with the kubelet
directory mounted `Bidirectional`), a controller `Deployment`
(`csi-provisioner` and `csi-attacher` plus the plugin) and, unless disabled, a
`StorageClass` named `etcfs`.

The controller provisions by creating a directory *through the filesystem*, so
it must be scheduled on a node that has the EtcFS mount. On a cluster where
only some nodes run EtcFS, label them and constrain both halves:

```bash
helm upgrade etcfs-csi ./csi/deploy/helm/etcfs-csi \
  --set controller.nodeSelector."etcfs\.io/member"=true \
  --set node.nodeSelector."etcfs\.io/member"=true
```

### Chart values

| Value | Default | Meaning |
| --- | --- | --- |
| `driverName` | `csi.etcfs.io` | CSI driver name; must match a StorageClass `provisioner` |
| `mountPath` | `/mnt/etcfs` | Where the EtcFS daemon mounts the filesystem on each host |
| `kubeletDir` | `/var/lib/kubelet` | Kubelet root; non-default on k3s and some OpenShift installs |
| `etcd.endpoints` | *(required)* | Comma-separated endpoints of the **EtcFS** etcd cluster |
| `etcd.tlsSecretName` | `""` | Secret with `ca.crt`, `tls.crt`, `tls.key` for etcd client TLS |
| `controller.replicaCount` | `1` | Controller replicas; leader election is off, so keep this at 1 |
| `controller.nodeSelector` | `{}` | Constrain the controller to nodes carrying the EtcFS mount |
| `node.tolerations` | `operator: Exists` | The node plugin runs everywhere by default |
| `storageClass.create` | `true` | Install a `StorageClass` alongside the driver |
| `storageClass.reclaimPolicy` | `Retain` | `Delete` removes the volume's directory and its contents |
| `image.repository` / `image.tag` | `ghcr.io/etcfs/etcfs-csi` / chart `appVersion` | Driver image |

## Using it

### Dynamic provisioning

A claim against the `etcfs` StorageClass gets a fresh directory in the shared
filesystem; every pod that mounts the claim, on any node, sees the same
contents.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: etcfs-shared
spec:
  accessModes: [ReadWriteMany]
  storageClassName: etcfs
  resources:
    requests:
      storage: 10Gi
```

A complete example with two writers pinned to different nodes is in
`csi/examples/dynamic-provisioning.yaml`.

### Static provisioning

To bind a `PersistentVolume` to a directory that already exists in the
filesystem, set `volumeHandle` to the directory name — a single path
component, no slashes:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: etcfs-datasets
spec:
  capacity:
    storage: 100Gi
  accessModes: [ReadWriteMany]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ""
  csi:
    driver: csi.etcfs.io
    volumeHandle: datasets
```

The full example, with its claim, is `csi/examples/static-provisioning.yaml`.
Volume handles are validated against a single-path-component pattern: a handle
containing a separator or a relative component is refused rather than
sanitised, because it is turned into a path under the mount root.

## Fencing, and what it means for Kubernetes

This is the part of the driver that is not boilerplate.

Kubernetes' own answer to "a node is gone but its volume is still attached" is
the `node.kubernetes.io/out-of-service` taint, GA since 1.28: a human asserts
that a dead node is really dead, and until they do, volumes stay attached and
pods stay `Terminating`. The assertion has to be made by a person because
Kubernetes has no way to *establish* the fact.

EtcFS establishes it automatically, in bounded time, through three layers
described in the [fencing architecture](../architecture/fencing/external-fencing-controller.md)
docs: a node that loses its membership lease stops writing on its own; peers
detach its volume and confirm the detachment before acting on it; and
generation-stamped extents catch anything that slipped through. The CSI driver
does not reimplement any of that. It contributes exactly one thing:

- **`ControllerUnpublishVolume` records a fence intent — but only for a node
  that no longer holds a membership lease.** A healthy node releasing a volume
  during ordinary pod rescheduling is left alone; fencing it would take out a
  working host. A node whose lease is gone is one the cluster has already
  stopped trusting, and the intent is picked up by
  `pkg/fencing.Controller`'s reconciliation sweep, which completes it with
  dual confirmation and cluster-wide claim deduplication.

- **`ControllerPublishVolume` refuses a node that is not a live member.** A pod
  is not placed on a host whose daemon is down or which has been fenced, where
  the mount path would be missing or empty.

The volume fence is deliberately **not** wired to the node taint. EtcFS
establishes a resource-scoped fact (this node can no longer write to this
device); the taint asserts a node-scoped one (this node is gone). A node
partitioned from etcd and EBS while still serving traffic is an ordinary
failure, and force-deleting its pods on the strength of a volume detach would
be unsound.

## Verifying an installation

```bash
kubectl -n kube-system get pods -l app.kubernetes.io/name=etcfs-csi
kubectl get csidrivers csi.etcfs.io
kubectl apply -f csi/examples/dynamic-provisioning.yaml
kubectl get pvc etcfs-shared
kubectl logs -l app=etcfs-writer --prefix --tail=20
```

Each writer pod lists the directory every five seconds; within one interval,
each should see the other's log file. On the hosts, the same files appear under
`/mnt/etcfs/pvc-<uid>/`, and `etcfsctl quota` reports usage against the claim's
requested size.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `FailedPrecondition: /mnt/etcfs is not a mount point on this node` | The EtcFS daemon is not running on that host, or mounts elsewhere; check `mountPath` |
| `FailedPrecondition: node <name> holds no EtcFS membership lease` | The node's daemon is down or fenced, or its `--node-id` differs from `spec.nodeName` |
| Pods stuck in `ContainerCreating`, kubelet reporting no plugin socket | `kubeletDir` does not match this distribution's kubelet root |
| Volume mounts but is empty on one node only | Mount propagation: the host mount must be visible in the plugin container (`HostToContainer`) and the plugin's bind mounts must propagate back (`Bidirectional`) |
| `CreateVolume` fails with a path error | The controller was scheduled onto a node without the EtcFS mount; use `controller.nodeSelector` |

## Not supported

Snapshots (`CreateSnapshot`), volume expansion and topology constraints are not
implemented. Snapshots in particular are not an oversight but a design
constraint — the metadata half is nearly free and the data half is not; see
`docs/design-decisions.md`. Volume expansion is meaningless while capacity is
cluster-wide and quotas are soft.
