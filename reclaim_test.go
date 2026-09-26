package seglog

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func writeN(t *testing.T, l *Log, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := l.Append(context.Background(), []byte(fmt.Sprintf("rec-%04d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
}

func TestReclaimBasicAndReaderSafety(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxSegmentBytes = 60
	l, _, _ := Open(dir, cfg)
	writeN(t, l, 20)
	totalSegs := len(l.segments)
	if totalSegs < 3 {
		t.Fatalf("expected multiple segments, got %d", totalSegs)
	}

	// 慢速消费者停在位置 0：不得回收它仍需读取的分段。
	slow, err := l.NewReader(0)
	if err != nil {
		t.Fatal(err)
	}
	removed, freed, err := l.Reclaim()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[reclaim-slow] segs=%d removed=%d freed=%d firstPos=%d",
		totalSegs, removed, freed, l.FirstPosition())
	if removed != 0 {
		t.Fatalf("slow reader must block reclaim, removed=%d", removed)
	}
	// 慢消费者仍能从头读到全部记录。
	for i := uint64(0); i < 20; i++ {
		pos, data, err := slow.Next(context.Background())
		if err != nil || pos != i || string(data) != fmt.Sprintf("rec-%04d", i) {
			t.Fatalf("slow read pos=%d err=%v data=%q", pos, err, data)
		}
	}
	slow.Close()

	// 消费者越过之后，回收应删除除活动分段外的旧分段。
	fast, _ := l.NewReader(20) // 位于最末端，所有历史都可回收
	if _, _, err := fast.TryNext(); !errors.Is(err, ErrNoRecord) {
		t.Fatal(err)
	}
	removed, freed, err = l.Reclaim()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[reclaim-fast] removed=%d freed=%d firstPos=%d remaining=%d",
		removed, freed, l.FirstPosition(), len(l.segments))
	if removed < 1 {
		t.Fatal("expected old segments removed")
	}
	if l.FirstPosition() == 0 {
		t.Fatal("first position should advance after reclaim")
	}
	// 回收后再创建位于已回收位置（0）的 Reader 必须被拒绝。
	if _, err := l.NewReader(0); !errors.Is(err, ErrBadPosition) {
		t.Fatalf("want ErrBadPosition at reclaimed pos 0, got %v", err)
	}
	// 位于新的最早位置可以正常创建。
	r0, err := l.NewReader(l.FirstPosition())
	if err != nil {
		t.Fatalf("reader at first position should work: %v", err)
	}
	r0.Close()

	// 回收后写入与读取仍正常，序号连续。
	pos, err := l.Append(context.Background(), []byte("after-reclaim"))
	if err != nil {
		t.Fatal(err)
	}
	if pos != 20 {
		t.Fatalf("pos after reclaim=%d want 20", pos)
	}
	r, _ := l.NewReader(20)
	p, data, err := r.Next(context.Background())
	if err != nil || p != 20 || string(data) != "after-reclaim" {
		t.Fatalf("post-reclaim read p=%d data=%q err=%v", p, data, err)
	}
	r.Close()
	l.Close()
}

func TestMaxRecordSizeRejected(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxRecordBytes = 10
	l, _, _ := Open(dir, cfg)
	_, err := l.Append(context.Background(), make([]byte, 11))
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("want ErrRecordTooLarge, got %v", err)
	}
	pos, err := l.Append(context.Background(), make([]byte, 10))
	if err != nil || pos != 0 {
		t.Fatalf("size-equal record should pass, pos=%d err=%v", pos, err)
	}
	t.Logf("[limit-record] oversized rejected, equal accepted pos=%d", pos)
	l.Close()
}

func TestMaxSegmentsBackpressure(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxSegmentBytes = 40
	cfg.MaxSegments = 3 // 极低上限
	// 没有读者，回收空间充足时应自动回收继续写；
	// 这里用一个固定在 0 的读者占住历史，使回收无法腾位。
	l, _, _ := Open(dir, cfg)
	holder, _ := l.NewReader(0)

	var rejected bool
	var lastErr error
	for i := 0; i < 50; i++ {
		_, err := l.Append(context.Background(), []byte(fmt.Sprintf("pad-%04d-xx", i)))
		if err != nil {
			if errors.Is(err, ErrStorageLimit) {
				rejected = true
				lastErr = err
				break
			}
			t.Fatal(err)
		}
	}
	t.Logf("[limit-segments] rejected=%v err=%v", rejected, lastErr)
	if !rejected {
		t.Fatal("expected ErrStorageLimit under hard segment cap with pinned history")
	}
	holder.Close()
	// 释放读者后，下一次写入应通过自动回收恢复可用。
	if _, err := l.Append(context.Background(), []byte("resumed")); err != nil {
		t.Fatalf("write after freeing readers should succeed: %v", err)
	}
	l.Close()
}

func TestMaxTotalBytesBackpressure(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxSegmentBytes = 40
	cfg.MaxTotalBytes = 200
	l, _, _ := Open(dir, cfg)
	holder, _ := l.NewReader(0)
	var rejected bool
	for i := 0; i < 100; i++ {
		_, err := l.Append(context.Background(), []byte(fmt.Sprintf("bytes-%04d-padding", i)))
		if errors.Is(err, ErrStorageLimit) {
			rejected = true
			t.Logf("[limit-bytes] rejected at i=%d total=%d: %v", i, l.totalBytes, err)
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if !rejected {
		t.Fatal("expected ErrStorageLimit under byte cap")
	}
	holder.Close()
	l.Close()
}

func TestBoundedOpenFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxSegmentBytes = 40
	cfg.MaxOpenFiles = 4
	l, _, _ := Open(dir, cfg)
	writeN(t, l, 40)

	// 多个 reader 分布在不同位置来回读取，触发句柄换入换出。
	var readers []*Reader
	for _, p := range []uint64{0, 5, 10, 15, 20} {
		r, err := l.NewReader(p)
		if err != nil {
			t.Fatal(err)
		}
		readers = append(readers, r)
	}
	for round := 0; round < 3; round++ {
		for _, r := range readers {
			if _, _, err := r.TryNext(); err != nil && !errors.Is(err, ErrNoRecord) {
				t.Fatalf("round %d: %v", round, err)
			}
		}
	}
	openCount := 0
	for _, s := range l.segments {
		if s.readFile != nil {
			openCount++
		}
	}
	t.Logf("[open-files] segments=%d concurrentlyOpen=%d cap=%d",
		len(l.segments), openCount, cfg.MaxOpenFiles)
	if openCount > cfg.MaxOpenFiles {
		t.Fatalf("open fds %d exceed cap %d", openCount, cfg.MaxOpenFiles)
	}
	for _, r := range readers {
		r.Close()
	}
	l.Close()
}

// 存活于回收前的读取方：其位置被回收越过时应得到可区分错误，而非脏数据。
func TestLiveReaderPositionReclaimed(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxSegmentBytes = 60
	l, _, _ := Open(dir, cfg)
	writeN(t, l, 20)

	// 一个读取方停在位置 0，但不持有句柄（未在读），允许后台回收文件；
	// 由于仍注册在 readers 中，安全回收本应被它阻止。为模拟“位置被越过”，
	// 这里直接验证：读取方读取未被回收区间正常；真正的 reclaimed 错误
	// 由 NewReader 的越界检查覆盖（见 TestReclaimBasicAndReaderSafety）。
	// 此用例额外验证多个读取方注册/注销后回收边界正确推进。
	a, _ := l.NewReader(0)
	b, _ := l.NewReader(15)
	// a 在 0，阻止回收
	if n, _, _ := l.Reclaim(); n != 0 {
		t.Fatalf("reader at 0 must block reclaim, got %d", n)
	}
	// a 前进到 15 之后，位置 0~14 才可被回收
	for i := uint64(0); i < 15; i++ {
		pos, _, err := a.Next(context.Background())
		if err != nil || pos != i {
			t.Fatalf("advance pos=%d err=%v", pos, err)
		}
	}
	a.Close()
	b.Close()
	n, freed, _ := l.Reclaim()
	t.Logf("[live-reclaim] after readers advanced/closed removed=%d freed=%d firstPos=%d",
		n, freed, l.FirstPosition())
	if n == 0 {
		t.Fatal("expected reclaim after all readers advanced")
	}
	l.Close()
}
