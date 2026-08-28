/*
Package main is etcfs-csi, the EtcFS Container Storage Interface plugin.

One binary serves both CSI services; --mode selects which. The node service
runs as a DaemonSet beside the EtcFS daemon and bind mounts volume
directories; the controller service runs as a Deployment, provisions those
directories and routes a departed node's volume release into the existing
fence path.

Usage:

	etcfs-csi --mode=node       --node-id=$NODE_NAME --mount-path=/mnt/etcfs --endpoint=unix:///csi/csi.sock
	etcfs-csi --mode=controller --node-id=$NODE_NAME --mount-path=/mnt/etcfs --etcd-endpoints=https://etcd:2379
*/
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/csi/internal/driver"
	"github.com/etcfs/etcfs/pkg/metadata"
)

// version is stamped at build time with -ldflags -X, mirroring the daemon.
var version = "dev"

func main() {
	var (
		mode          = flag.String("mode", "node", "Which CSI services to serve: node, controller or all")
		endpoint      = flag.String("endpoint", "unix:///csi/csi.sock", "CSI gRPC endpoint")
		nodeID        = flag.String("node-id", "", "This node's identifier; must match the EtcFS daemon's --node-id (default: hostname)")
		mountPath     = flag.String("mount-path", "/mnt/etcfs", "Where the EtcFS filesystem is mounted; volumes are subdirectories of it")
		driverName    = flag.String("driver-name", driver.DefaultName, "CSI driver name, matching the StorageClass provisioner")
		etcdEndpoints = flag.String("etcd-endpoints", "", "Comma-separated etcd client endpoints (controller only)")
		etcdCert      = flag.String("etcd-cert", "", "Path to etcd client certificate")
		etcdKey       = flag.String("etcd-key", "", "Path to etcd client key")
		etcdCA        = flag.String("etcd-ca", "", "Path to etcd CA certificate")
		showVersion   = flag.Bool("version", false, "Print version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}
	if *nodeID == "" {
		hostname, _ := os.Hostname()
		*nodeID = hostname
	}

	cfg := driver.Config{
		Name:          *driverName,
		Version:       version,
		Mode:          driver.Mode(*mode),
		Endpoint:      *endpoint,
		NodeID:        *nodeID,
		MountPath:     *mountPath,
		EtcdEndpoints: splitNonEmpty(*etcdEndpoints),
		EtcdCertFile:  *etcdCert,
		EtcdKeyFile:   *etcdKey,
		EtcdCAFile:    *etcdCA,
	}

	var store *metadata.Store
	if cfg.Mode != driver.ModeNode {
		if len(cfg.EtcdEndpoints) == 0 {
			fatalf("--etcd-endpoints is required in %s mode", cfg.Mode)
		}
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   cfg.EtcdEndpoints,
			DialTimeout: 5 * time.Second,
			TLS:         tlsConfig(cfg.EtcdCertFile, cfg.EtcdKeyFile, cfg.EtcdCAFile),
		})
		if err != nil {
			fatalf("connect to etcd: %v", err)
		}
		defer func() { _ = cli.Close() }()
		store = metadata.NewStore(cli, cfg.NodeID)
	}

	d, err := driver.New(cfg, store)
	if err != nil {
		fatalf("%v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Printf("etcfs-csi %s: mode=%s node=%s mount=%s endpoint=%s\n",
		version, cfg.Mode, cfg.NodeID, cfg.MountPath, cfg.Endpoint)
	if err := d.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fatalf("serve: %v", err)
	}
}

func splitNonEmpty(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return strings.Split(s, ",")
}

func tlsConfig(certFile, keyFile, caFile string) *tls.Config {
	if certFile == "" && caFile == "" {
		return nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			fatalf("load etcd client certificate: %v", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			fatalf("read etcd CA: %v", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			fatalf("etcd CA %s contains no certificates", caFile)
		}
		cfg.RootCAs = pool
	}
	return cfg
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "etcfs-csi: "+format+"\n", args...)
	os.Exit(1)
}
