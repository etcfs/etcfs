// Command verify-history checks one or more recorded operation histories
// (etcfuse-meta --history-log) against the consistency models in
// test/verify. See docs/verification/porcupine.md.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/etcfs/etcfs/internal/history"
	"github.com/etcfs/etcfs/test/verify"
)

func main() {
	var files, models, crashed string
	var timeout time.Duration
	flag.StringVar(&files, "files", "", "comma-separated history files, one per node")
	flag.StringVar(&models, "models", allModels,
		"comma-separated models to check: "+allModels)
	flag.StringVar(&crashed, "crashed", "",
		"comma-separated nodes that were killed rather than shut down, whose unflushed writes may be lost")
	flag.DurationVar(&timeout, "timeout", 5*time.Minute, "per-model checker timeout")
	flag.Parse()

	if files == "" {
		fmt.Fprintln(os.Stderr, "usage: verify-history --files=history-n1.jsonl,history-n2.jsonl [--models="+allModels+"] [--crashed=n1]")
		os.Exit(2)
	}

	var entries []history.Entry
	for _, path := range strings.Split(files, ",") {
		e, err := history.Load(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load %s: %v\n", path, err)
			os.Exit(1)
		}
		fmt.Printf("%s: %d entries\n", path, len(e))
		entries = append(entries, e...)
	}

	failed := false
	for _, model := range strings.Split(models, ",") {
		ok, err := checkModel(strings.TrimSpace(model), entries, split(crashed), timeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", model, err)
			failed = true
			continue
		}
		if !ok {
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}

// allModels is every model this command knows, and the default set.
const allModels = "namespace,extent,lock,lockkey,block,pagecache,generation"

// split turns a comma-separated flag into its non-empty parts.
func split(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func checkModel(model string, entries []history.Entry, crashed []string, timeout time.Duration) (bool, error) {
	switch model {
	case "namespace":
		ops, err := verify.DecodeNamespace(entries)
		if err != nil {
			return false, err
		}
		fmt.Printf("namespace: checking %d operations\n", len(ops))
		return report("namespace", verify.Check(verify.NamespaceModel, verify.Operations(ops), verify.AllLinearizable, 0, timeout))
	case "extent":
		ops, err := verify.DecodeExtents(entries)
		if err != nil {
			return false, err
		}
		fmt.Printf("extent: checking %d operations\n", len(ops))
		return report("extent", verify.CheckExtents(ops, crashed, timeout))
	case "lock":
		ops, err := verify.DecodeLocks(entries)
		if err != nil {
			return false, err
		}
		fmt.Printf("lock: checking %d events\n", len(ops))
		return report("lock", verify.CheckLocks(ops, verify.DecodeStarts(entries), verify.DefaultLockLeaseTTL, timeout))
	case "lockkey":
		ops, err := verify.DecodeLockKeys(entries)
		if err != nil {
			return false, err
		}
		fmt.Printf("lockkey: checking %d events\n", len(ops))
		return report("lockkey", verify.CheckLocks(ops, verify.DecodeStarts(entries), verify.DefaultLockLeaseTTL, timeout))
	case "block":
		ops, err := verify.DecodeBlocks(entries)
		if err != nil {
			return false, err
		}
		fmt.Printf("block: checking %d events\n", len(ops))
		return report("block", verify.CheckBlocks(ops, verify.DecodeStarts(entries), timeout))
	case "pagecache":
		keys, err := verify.DecodeLockKeys(entries)
		if err != nil {
			return false, err
		}
		invals, err := verify.DecodePageInvals(entries)
		if err != nil {
			return false, err
		}
		fmt.Printf("pagecache: checking %d releases against %d invalidations\n", len(keys), len(invals))
		violations := verify.CheckPageCache(keys, invals)
		for _, v := range violations {
			fmt.Printf("pagecache: %s\n", v)
		}
		if len(violations) > 0 {
			fmt.Println("pagecache: VIOLATION")
			return false, nil
		}
		fmt.Println("pagecache: OK")
		return true, nil
	case "generation":
		ops, err := verify.DecodeGuardedCommits(entries)
		if err != nil {
			return false, err
		}
		fmt.Printf("generation: checking %d commits\n", len(ops))
		return report("generation", verify.CheckGenerations(ops, timeout))
	}
	return false, fmt.Errorf("unknown model %q", model)
}

func report(name string, res porcupine.CheckResult) (bool, error) {
	switch res {
	case porcupine.Ok:
		fmt.Printf("%s: OK\n", name)
		return true, nil
	case porcupine.Unknown:
		fmt.Printf("%s: TIMED OUT\n", name)
		return false, nil
	default:
		fmt.Printf("%s: VIOLATION\n", name)
		return false, nil
	}
}
