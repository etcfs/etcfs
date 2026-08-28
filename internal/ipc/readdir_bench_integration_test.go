//go:build integration
// +build integration

package ipc

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/etcfs/etcfs/pkg/metadata"
)

// What one full scan of a directory costs, measured the way the kernel drives
// it: repeated READDIR calls with the buffer size it actually passes, each
// resuming at the cookie the last reply ended on, until the listing runs out.
func scanDirectory(t *testing.T, svc *Service, ino uint64, size uint32, plus bool) (calls, entries int) {
	t.Helper()
	op := ipcOpReaddir
	if plus {
		op = ipcOpReadDirPlus
	}
	for offset := uint64(0); ; calls++ {
		resp, err := svc.dispatch(uint16(op), readdirPayloadFor(ino, offset, size))
		if err != nil {
			t.Fatalf("readdir at offset %d: %v", offset, err)
		}
		if errno := int32(binary.BigEndian.Uint32(resp[0:4])); errno != 0 {
			t.Fatalf("readdir at offset %d: errno %d", offset, errno)
		}
		n := int(binary.BigEndian.Uint32(resp[4:8]))
		if n == 0 {
			return calls, entries
		}
		entries += n
		offset += uint64(n)
		if calls > 200000 {
			t.Fatal("the scan did not terminate")
		}
	}
}

// The cost of listing a directory, as a function of its size.
//
// Each READDIR reads the whole directory out of etcd and then returns the one
// page it was asked for, so a scan does one full prefix scan per page and the
// total work grows with the square of the directory size. The numbers this
// prints are what the pagination work is measured against; the assertion is
// only that a scan terminates and returns every entry, since the timings depend
// on the etcd it is run against.
func TestReaddirScanCost(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()

	for _, n := range []int{100, 1000, 5000} {
		dirIno := uint64(900000 + n)
		if _, err := store.AtomicCreateDir(ctx, metadata.RootIno,
			fmt.Sprintf("scan-%d", n), dirIno, 0o40755, 1000, 1000); err != nil {
			t.Fatalf("create directory: %v", err)
		}
		for i := 0; i < n; i++ {
			name := fmt.Sprintf("entry-%06d", i)
			if _, err := store.AtomicCreateFile(ctx, dirIno, name,
				dirIno*100+uint64(i), 0o100644, 1000, 1000, metadata.CreateExtra{}); err != nil {
				t.Fatalf("seed %s: %v", name, err)
			}
		}

		for _, plus := range []bool{false, true} {
			before := rescanCount(t)
			start := time.Now()
			calls, got := scanDirectory(t, svc, dirIno, 4096, plus)
			elapsed := time.Since(start)
			if got != n {
				t.Errorf("n=%d plus=%v: scanned %d entries, want %d", n, plus, got, n)
			}
			rescans := rescanCount(t) - before
			t.Logf("n=%5d plus=%-5v %4d calls, %8.1fms, %d full directory reads",
				n, plus, calls, float64(elapsed.Microseconds())/1000, rescans)

			// A sequential scan reads the directory from the start once, for
			// its first page, and resumes for every page after it. More than
			// one full read means the scan is paying per page again, which is
			// the quadratic this exists to remove.
			if rescans != 1 {
				t.Errorf("n=%d plus=%v: %d full directory reads, want exactly 1",
					n, plus, rescans)
			}
		}
	}
}

// scanNames drives a full scan and returns the names in the order they came
// back, which is the thing pagination must not change.
func scanNames(t *testing.T, svc *Service, ino uint64, size uint32, plus bool) []string {
	t.Helper()
	op := ipcOpReaddir
	if plus {
		op = ipcOpReadDirPlus
	}
	var names []string
	for offset := uint64(0); ; {
		resp, err := svc.dispatch(uint16(op), readdirPayloadFor(ino, offset, size))
		if err != nil {
			t.Fatalf("readdir at offset %d: %v", offset, err)
		}
		r := newReader(resp[4:])
		n := r.u32()
		if n == 0 {
			return names
		}
		for i := uint32(0); i < n; i++ {
			r.u64() // ino
			names = append(names, r.str())
			r.u32() // type
			r.u64() // cookie
			if plus {
				r.take(attrWireSize + 8) // attr block and the two timeouts
			}
		}
		if !r.ok {
			t.Fatalf("readdir at offset %d: reply is truncated", offset)
		}
		offset += uint64(n)
	}
}

// Paging is an optimisation, so it has to be invisible: a scan must return
// every name exactly once, in sorted order, whatever buffer size the kernel
// picks — and the same names a single unpaged listing would have given.
func TestPagedScanReturnsEveryNameExactlyOnce(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()

	const n = 500
	const dirIno = 880001
	if _, err := store.AtomicCreateDir(ctx, metadata.RootIno, "paged", dirIno, 0o40755, 1000, 1000); err != nil {
		t.Fatalf("create directory: %v", err)
	}
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		// Deliberately uneven name lengths: a fixed width would hide an error
		// in the per-entry size estimate the page limit is derived from.
		name := fmt.Sprintf("f%0*d", 1+i%20, i)
		if _, err := store.AtomicCreateFile(ctx, dirIno, name, dirIno*100+uint64(i), 0o100644, 1000, 1000, metadata.CreateExtra{}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
		want = append(want, name)
	}
	sort.Strings(want)

	for _, size := range []uint32{4096, 8192, 65536} {
		for _, plus := range []bool{false, true} {
			svc.dirCursors.forget(dirIno) // start cold, so page one takes the fallback
			got := scanNames(t, svc, dirIno, size, plus)
			if !slices.Equal(got, want) {
				t.Errorf("size=%d plus=%v: scanned %d names, want %d (first difference at %s)",
					size, plus, len(got), len(want), firstDifference(got, want))
			}
		}
	}
}

// A scan that does not resume where the last reply stopped — a seekdir, or two
// processes reading one directory at once — must still be answered correctly,
// from the fallback that counts rather than resumes.
func TestScanIsCorrectWithoutACursor(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()

	const n = 200
	const dirIno = 880002
	if _, err := store.AtomicCreateDir(ctx, metadata.RootIno, "nocursor", dirIno, 0o40755, 1000, 1000); err != nil {
		t.Fatalf("create directory: %v", err)
	}
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("entry-%04d", i)
		if _, err := store.AtomicCreateFile(ctx, dirIno, name, dirIno*100+uint64(i), 0o100644, 1000, 1000, metadata.CreateExtra{}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
		want = append(want, name)
	}
	sort.Strings(want)

	// Every page starts with the cursor dropped, so nothing ever resumes.
	var got []string
	for offset := uint64(0); ; {
		svc.dirCursors.forget(dirIno)
		resp, err := svc.dispatch(uint16(ipcOpReaddir), readdirPayloadFor(dirIno, offset, 4096))
		if err != nil {
			t.Fatalf("readdir: %v", err)
		}
		r := newReader(resp[4:])
		count := r.u32()
		if count == 0 {
			break
		}
		for i := uint32(0); i < count; i++ {
			r.u64()
			got = append(got, r.str())
			r.u32()
			r.u64()
		}
		offset += uint64(count)
	}
	if !slices.Equal(got, want) {
		t.Errorf("without a cursor: scanned %d names, want %d (first difference at %s)",
			len(got), len(want), firstDifference(got, want))
	}
}

func firstDifference(got, want []string) string {
	for i := range want {
		if i >= len(got) {
			return fmt.Sprintf("index %d: missing %q", i, want[i])
		}
		if got[i] != want[i] {
			return fmt.Sprintf("index %d: got %q, want %q", i, got[i], want[i])
		}
	}
	if len(got) > len(want) {
		return fmt.Sprintf("index %d: unexpected %q", len(want), got[len(want)])
	}
	return "none"
}

// rescanCount reads the counter of READDIR pages that had to read the whole
// directory rather than resume, straight out of the registry the daemon
// publishes — so the test measures what the daemon actually did instead of
// re-deriving it from the reply shape.
func rescanCount(t *testing.T) int {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, f := range families {
		if f.GetName() != "etcfuse_readdir_page_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "result" && l.GetValue() == "rescanned" {
					return int(m.GetCounter().GetValue())
				}
			}
		}
	}
	return 0
}
