package seglog

import (
	"context"
	"fmt"
	"testing"
)

func TestSegmentRollAndChunks(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxSegmentBytes = 100 // 极小分段，强制滚动与多帧拆分
	cfg.MaxChunkBytes = 40
	l, info, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("open: committed=%d segments=%d", info.CommittedPosition, len(info.Segments))

	// 多条小记录 -> 触发滚动；一条大记录 -> 触发多帧拆分。
	var positions []uint64
	for i := 0; i < 10; i++ {
		p, err := l.Append(context.Background(), []byte(fmt.Sprintf("small-%02d", i)))
		if err != nil {
			t.Fatal(err)
		}
		positions = append(positions, p)
	}
	big := make([]byte, 250)
	for i := range big {
		big[i] = byte('A' + i%26)
	}
	bigPos, err := l.Append(context.Background(), big)
	if err != nil {
		t.Fatal(err)
	}
	positions = append(positions, bigPos)

	r, err := l.NewReader(0)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range positions {
		pos, data, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if pos != want {
			t.Fatalf("pos=%d want %d", pos, want)
		}
		if i < 10 {
			if string(data) != fmt.Sprintf("small-%02d", i) {
				t.Fatalf("data mismatch %q", data)
			}
		} else if string(data) != string(big) {
			t.Fatalf("big record mismatch len=%d", len(data))
		}
	}
	t.Logf("segments after writes=%d committed=%d", len(l.segments), l.CommittedPosition())
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启后应得到完全一致的数据。
	l2, ri, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("reopen: committed=%d kind=%s segments=%d reason=%q",
		ri.CommittedPosition, ri.Corruption, len(ri.Segments), ri.Reason)
	if ri.CommittedPosition != 11 {
		t.Fatalf("committed=%d want 11", ri.CommittedPosition)
	}
	r2, _ := l2.NewReader(0)
	for i, want := range positions {
		pos, _, err := r2.Next(context.Background())
		if err != nil {
			t.Fatalf("reopen record %d: %v", i, err)
		}
		if pos != want {
			t.Fatalf("reopen pos=%d want %d", pos, want)
		}
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}
}
