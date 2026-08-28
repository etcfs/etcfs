package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/pkg/quota"
)

// runQuota dispatches the quota subcommands: no argument reports usage, "set"
// makes a directory a quota root, "clear" removes one.
func runQuota(ctx context.Context, store *metadata.Store, args []string, asJSON bool) error {
	if len(args) == 0 {
		return reportQuotas(ctx, store, asJSON)
	}
	switch args[0] {
	case "set":
		return setQuota(ctx, store, args[1:])
	case "clear":
		if len(args) != 2 {
			return fmt.Errorf("quota clear takes exactly one inode")
		}
		ino, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid inode %q: %w", args[1], err)
		}
		if err := store.ClearQuota(ctx, ino); err != nil {
			return err
		}
		fmt.Printf("quota cleared on inode %d\n", ino)
		return nil
	}
	return fmt.Errorf("unknown quota subcommand %q; expected set or clear", args[0])
}

func setQuota(ctx context.Context, store *metadata.Store, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("quota set takes an inode and at least one of --bytes or --inodes")
	}
	ino, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid inode %q: %w", args[0], err)
	}

	var rec metadata.QuotaRecord
	for _, arg := range args[1:] {
		name, value, found := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		if !found {
			return fmt.Errorf("expected --bytes=N or --inodes=N, got %q", arg)
		}
		n, perr := strconv.ParseUint(value, 10, 64)
		if perr != nil {
			return fmt.Errorf("invalid value for --%s: %w", name, perr)
		}
		switch name {
		case "bytes":
			rec.Bytes = n
		case "inodes":
			rec.Inodes = n
		default:
			return fmt.Errorf("unknown flag --%s; expected bytes or inodes", name)
		}
	}
	if rec.Bytes == 0 && rec.Inodes == 0 {
		return fmt.Errorf("quota set needs at least one of --bytes or --inodes above zero")
	}

	if err := store.SetQuota(ctx, ino, rec); err != nil {
		return err
	}
	fmt.Printf("quota set on inode %d: bytes=%d inodes=%d\n", ino, rec.Bytes, rec.Inodes)
	fmt.Println(quotaAdvisoryNote)
	return nil
}

// quotaAdvisoryNote is printed wherever a limit is set or reported, because a
// limit that looks like a limit and rejects nothing is the kind of thing an
// operator finds out about from a full filesystem.
const quotaAdvisoryNote = "note: quotas are advisory — usage is reported, and no write is rejected for exceeding a limit"

// reportQuotas computes usage for every quota root.
//
// The namespace walk is done here, in a one-shot command, rather than on the
// write path: charging a write to its subtree as it happens would need the
// enclosing root known per write, and the write path is already bound by how
// many Raft round trips it makes. These are therefore soft quotas, and the
// output says as much rather than implying enforcement that does not exist.
func reportQuotas(ctx context.Context, store *metadata.Store, asJSON bool) error {
	limits, err := store.ListQuotas(ctx)
	if err != nil {
		return err
	}
	if len(limits) == 0 {
		if asJSON {
			return printJSON([]quota.Usage{})
		}
		fmt.Println("no quota roots configured")
		return nil
	}

	direntKvs, err := store.GetPrefix(ctx, metadata.PrefixDirent)
	if err != nil {
		return fmt.Errorf("read namespace: %w", err)
	}
	keys := make([]string, len(direntKvs))
	targets := make([]uint64, len(direntKvs))
	for i, kv := range direntKvs {
		keys[i] = string(kv.Key)
		targets[i] = metadata.DecodeUint64(kv.Value)
	}

	inodeKvs, err := store.GetPrefix(ctx, metadata.PrefixInode)
	if err != nil {
		return fmt.Errorf("read inodes: %w", err)
	}
	inodes := make(map[uint64]*metadata.InodeRecord, len(inodeKvs))
	for _, kv := range inodeKvs {
		if rec := metadata.DecodeInode(kv.Value); rec != nil {
			inodes[rec.Ino] = rec
		}
	}

	lims := make(map[uint64]quota.Limits, len(limits))
	for ino, rec := range limits {
		lims[ino] = quota.Limits{Bytes: rec.Bytes, Inodes: rec.Inodes}
	}

	usages := quota.Compute(quota.BuildTree(keys, targets), inodes, lims)
	sort.Slice(usages, func(i, j int) bool { return usages[i].Root < usages[j].Root })

	if asJSON {
		return printJSON(usages)
	}
	for _, u := range usages {
		line := u.String()
		if u.OverBytes() || u.OverInodes() {
			line += "  OVER LIMIT"
		}
		fmt.Println(line)
	}
	fmt.Println(quotaAdvisoryNote)
	return nil
}
