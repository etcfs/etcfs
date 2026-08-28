/*
Package main is etcfsctl, the EtcFS operator CLI.

fsck and filesystem info exist today only as flags on the daemon binary
(etcfuse-meta --fsck, --info), so diagnosing a filesystem means running the
thing that mounts it with a special flag. etcfsctl is a front door onto the
same underlying packages (pkg/fsck, pkg/fsinfo, pkg/membership, pkg/arena,
pkg/fencing) with no daemon involved.

Usage:

	etcfsctl [flags] <command> [args]

Commands:

	status           Filesystem-wide statistics (inodes, extents, arenas, members)
	members          List cluster members
	arenas           List arena ownership and utilization
	fsck             Run the offline filesystem checker
	scrub            Run one scrub pass and report anomalies
	fence <node-id>  Record a fence intent for a departed node
	quota            Report usage against every quota root
	quota set <ino> --bytes=N --inodes=N   Make a directory a quota root
	quota clear <ino>                      Remove a quota root

Flags (before the command):

	--etcd-endpoints  Comma-separated etcd client endpoints (default http://localhost:2379)
	--etcd-cert, --etcd-key, --etcd-ca  etcd client TLS
	--timeout         Bound on the etcd work behind one command (default 10s)
	--json            Emit machine-readable JSON instead of text
*/
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/pkg/fsck"
	"github.com/etcfs/etcfs/pkg/fsinfo"
	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/pkg/scrub"
)

func main() {
	var etcdEndpoints, etcdCert, etcdKey, etcdCA string
	var asJSON bool
	var timeout time.Duration

	// --json is pulled out of os.Args before flag.Parse rather than declared
	// as an ordinary flag. flag.Parse stops at the first positional argument,
	// so a flag placed after the subcommand — "etcfsctl status --json", the
	// natural way to type it, or "quota set 10 --json --bytes=5" — would
	// otherwise sit unparsed in the leftover args and be silently ignored
	// instead of honoured or rejected. Stripping it wherever it appears, before
	// any positional parsing happens, is what makes it order-independent.
	rawArgs, jsonRequested := stripJSONFlag(os.Args[1:])
	asJSON = jsonRequested

	flag.StringVar(&etcdEndpoints, "etcd-endpoints", "http://localhost:2379",
		"Comma-separated etcd client endpoints")
	flag.StringVar(&etcdCert, "etcd-cert", "", "Path to etcd client certificate")
	flag.StringVar(&etcdKey, "etcd-key", "", "Path to etcd client key")
	flag.StringVar(&etcdCA, "etcd-ca", "", "Path to etcd CA certificate")
	flag.DurationVar(&timeout, "timeout", 10*time.Second,
		"Bound on the etcd work behind one command; a partitioned cluster would otherwise hang it indefinitely")
	flag.BoolVar(&asJSON, "json", asJSON, "Emit JSON instead of text")
	flag.Usage = usage
	if err := flag.CommandLine.Parse(rawArgs); err != nil {
		os.Exit(2)
	}

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	cmd, rest := args[0], args[1:]

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            strings.Split(etcdEndpoints, ","),
		DialTimeout:          3 * time.Second,
		DialKeepAliveTime:    1 * time.Second,
		DialKeepAliveTimeout: 1 * time.Second,
		PermitWithoutStream:  true,
		TLS:                  tlsConfig(etcdCert, etcdKey, etcdCA),
	})
	if err != nil {
		fatalf("connect to etcd: %v", err)
	}
	defer func() { _ = cli.Close() }()

	store := metadata.NewStore(cli, "etcfsctl")

	// Bounded for the same reason the daemon bounds a FUSE request: without a
	// deadline the etcd client retries a partitioned cluster indefinitely, and
	// an operator tool that hangs during an incident is worse than one that
	// fails and says why.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var runErr error
	switch cmd {
	case "status":
		runErr = runStatus(ctx, store, asJSON)
	case "members":
		runErr = runMembers(ctx, store, asJSON)
	case "arenas":
		runErr = runArenas(ctx, store, asJSON)
	case "fsck":
		runErr = runFsck(ctx, store, asJSON)
	case "scrub":
		runErr = runScrub(ctx, store, asJSON)
	case "fence":
		if len(rest) != 1 {
			fatalf("fence requires exactly one node-id argument")
		}
		runErr = runFence(ctx, store, rest[0])
	case "quota":
		runErr = runQuota(ctx, store, rest, asJSON)
	default:
		usage()
		os.Exit(2)
	}
	if runErr != nil {
		fatalf("%v", runErr)
	}
}

// stripJSONFlag removes every "--json" / "-json" token from args, wherever it
// appears, and reports whether one was found. The remaining tokens keep their
// relative order, so a value-bearing flag adjacent to --json (e.g.
// "--bytes=5 --json") is untouched.
func stripJSONFlag(args []string) (rest []string, found bool) {
	rest = make([]string, 0, len(args))
	for _, a := range args {
		if a == "--json" || a == "-json" {
			found = true
			continue
		}
		rest = append(rest, a)
	}
	return rest, found
}

func usage() {
	fmt.Fprintln(os.Stderr,
		"usage: etcfsctl [flags] <status|members|arenas|fsck|scrub|fence <node-id>|quota [set|clear] ...>")
	flag.PrintDefaults()
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "etcfsctl: "+format+"\n", args...)
	os.Exit(1)
}

func tlsConfig(cert, key, ca string) *tls.Config {
	if cert == "" && ca == "" {
		return nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if cert != "" && key != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			fatalf("load etcd client cert: %v", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	if ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			fatalf("read etcd CA cert: %v", err)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(pem)
		cfg.RootCAs = pool
	}
	return cfg
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func runStatus(ctx context.Context, store *metadata.Store, asJSON bool) error {
	info, err := fsinfo.Collect(ctx, store)
	if err != nil {
		return fmt.Errorf("collect filesystem info: %w", err)
	}
	if asJSON {
		return printJSON(info)
	}
	fmt.Println(info.String())
	return nil
}

type member struct {
	NodeID     string `json:"node_id"`
	InstanceID string `json:"instance_id,omitempty"`
}

func runMembers(ctx context.Context, store *metadata.Store, asJSON bool) error {
	kvs, err := store.GetPrefix(ctx, metadata.PrefixMembership)
	if err != nil {
		return fmt.Errorf("list members: %w", err)
	}
	members := make([]member, 0, len(kvs))
	for _, kv := range kvs {
		nodeID := strings.TrimPrefix(string(kv.Key), metadata.PrefixMembership)
		members = append(members, member{
			NodeID:     nodeID,
			InstanceID: metadata.InstanceIDFromMembership(kv.Value),
		})
	}
	if asJSON {
		return printJSON(members)
	}
	for _, m := range members {
		fmt.Printf("%s\tinstance=%s\n", m.NodeID, m.InstanceID)
	}
	return nil
}

func runArenas(ctx context.Context, store *metadata.Store, asJSON bool) error {
	info, err := fsinfo.Collect(ctx, store)
	if err != nil {
		return fmt.Errorf("collect arena info: %w", err)
	}
	if asJSON {
		return printJSON(info.ArenaUtilization)
	}
	for node, util := range info.ArenaUtilization {
		fmt.Printf("%s\tutilization=%.2f%%\n", node, util*100)
	}
	return nil
}

func runFsck(ctx context.Context, store *metadata.Store, asJSON bool) error {
	chk := fsck.New(store)
	findings := chk.Run(ctx)
	if asJSON {
		return printJSON(struct {
			Errors   int            `json:"errors"`
			Warnings int            `json:"warnings"`
			Findings []fsck.Finding `json:"findings"`
		}{chk.ErrorCount(), chk.WarningCount(), findings})
	}
	fmt.Printf("fsck: %d errors, %d warnings\n", chk.ErrorCount(), chk.WarningCount())
	for _, f := range findings {
		fmt.Printf("  [%s] %s\n", f.Level, f.Message)
	}
	return nil
}

// runScrub runs every scrub check that is answerable from metadata alone and
// reports what it finds without remediating anything.
//
// The range-validity check is the one omission: it compares extent offsets
// against the size of the real device, which this process has not opened and
// must not open while a node is writing to it. That check runs in the daemon's
// own scrubber, which has the device.
func runScrub(ctx context.Context, store *metadata.Store, asJSON bool) error {
	snap, err := scrub.Scan(ctx, store)
	if err != nil {
		return fmt.Errorf("scan: %w", err)
	}
	results := scrub.CheckExtentCollisions(snap)
	results = append(results, scrub.CheckOrphanExtents(snap)...)
	results = append(results, scrub.CheckDeadExtents(snap)...)
	results = append(results, scrub.CheckGenerationConsistency(snap)...)
	results = append(results, scrub.CheckNlinkConsistency(snap)...)
	results = append(results, scrub.CheckUnreferencedInodes(snap)...)
	if asJSON {
		return printJSON(results)
	}
	fmt.Printf("scrub: %d anomalies\n", len(results))
	for _, r := range results {
		fmt.Printf("  [%s] %s\n", r.Type, r.Detail)
	}
	return nil
}

// runFence records that a departed node is owed a fence, so a controller's
// reconciliation sweep picks it up and completes it.
//
// It refuses a node that still holds a live membership lease, and the refusal
// is the point rather than caution. Fencing here is lease-driven: a node with
// a live lease is one whose own self-fencing watchdog has not stopped it and
// which etcd can still reach, so the controller's sweep deliberately drops any
// intent recorded against it (see Controller.reconcile). Recording one anyway
// would be a no-op the operator was told had been scheduled — the worst
// possible answer during an incident. Stop the node, or let its lease expire,
// and the fence follows automatically.
//
// The fence itself is never performed here. A controller's sweep claims the
// intent and executes it, which keeps dual confirmation and the cluster-wide
// claim dedup in pkg/fencing.Controller rather than reimplemented without them.
func runFence(ctx context.Context, store *metadata.Store, nodeID string) error {
	value, err := store.Get(ctx, metadata.MembershipKey(nodeID))
	if err != nil {
		return fmt.Errorf("look up node %s: %w", nodeID, err)
	}
	if value != nil {
		return fmt.Errorf("node %s still holds a live membership lease; a fence intent against a "+
			"live node is dropped by the reconciliation sweep. Stop the node or wait for its lease "+
			"to expire, and it will be fenced automatically", nodeID)
	}

	// A departed node's membership key is already gone, so its instance ID went
	// with it: there is nothing left to detach and the fence degrades to a
	// generation bump. Recording the intent is still what makes the fence
	// survive a controller restart.
	if err := store.RecordFenceIntent(ctx, nodeID, ""); err != nil {
		return fmt.Errorf("record fence intent for %s: %w", nodeID, err)
	}
	fmt.Printf("fence recorded for departed node %s; a running fencing controller will complete it\n", nodeID)
	return nil
}
