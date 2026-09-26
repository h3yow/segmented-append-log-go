package seglog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func testConfig() Config {
	c := DefaultConfig()
	c.FsyncEveryWrite = true
	return c
}

func TestAppendReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l, info, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if info.CommittedPosition != 0 {
		t.Fatalf("fresh committed=%d want 0", info.CommittedPosition)
	}
	const n = 20
	for i := 0; i < n; i++ {
		pos, err := l.Append(context.Background(), []byte(fmt.Sprintf("record-%02d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if pos != uint64(i) {
			t.Fatalf("pos=%d want %d", pos, i)
		}
	}
	r, err := l.NewReader(0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		pos, data, err := r.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if pos != uint64(i) || string(data) != fmt.Sprintf("record-%02d", i) {
			t.Fatalf("got (%d,%q) want (%d,record-%02d)", pos, data, i, i)
		}
	}
	if _, _, err := r.TryNext(); !errors.Is(err, ErrNoRecord) {
		t.Fatalf("want ErrNoRecord, got %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReopenClean(t *testing.T) {
	dir := t.TempDir()
	l, _, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := l.Append(context.Background(), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l2, info, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("reopen recovery: committed=%d kind=%s reason=%q",
		info.CommittedPosition, info.Corruption, info.Reason)
	if info.CommittedPosition != 5 {
		t.Fatalf("committed=%d want 5", info.CommittedPosition)
	}
	if info.Corruption != CorruptionNone {
		t.Fatalf("unexpected corruption %s", info.Corruption)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentFilesExist(t *testing.T) {
	dir := t.TempDir()
	l, _, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	files, _ := os.ReadDir(dir)
	if len(files) != 1 {
		t.Fatalf("want 1 segment, got %d", len(files))
	}
	if _, ok := parseSegmentName(files[0].Name()); !ok {
		t.Fatalf("bad segment name %q", files[0].Name())
	}
	if filepath.Base(filepath.Join(dir, segmentName(0))) != files[0].Name() {
		t.Fatalf("name mismatch")
	}
}
