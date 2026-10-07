package seglog

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// 容量上限：总字节耗尽且消费进度不允许回收时，写入立即报 ErrNoCapacity；
// 消费者推进后，写入路径上的自动回收腾出空间，写入恢复。
func TestCapacityMaxTotalBytes(t *testing.T) {
	dir := t.TempDir()
	cfg := splitTestConfig(dir) // 64 字节段，每条记录 14 字节
	cfg.MaxTotalBytes = 192     // 约 3 个段
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	slow, err := l.RegisterConsumer(0) // 慢消费者：一直不推进
	if err != nil {
		t.Fatal(err)
	}
	var lastErr error
	var n int
	for ; n < 100; n++ {
		if _, err := l.Append([]byte(fmt.Sprintf("r%02d", n))); err != nil {
			lastErr = err
			break
		}
	}
	if !errors.Is(lastErr, ErrNoCapacity) {
		t.Fatalf("want ErrNoCapacity, got %v (appended %d)", lastErr, n)
	}
	if errors.Is(lastErr, ErrBackpressure) {
		t.Fatal("capacity exhaustion must be distinguishable from backpressure")
	}
	t.Logf("capacity exhausted after %d records, classified as ErrNoCapacity", n)

	// 慢消费者越过前两个段后，自动回收腾地方，写入恢复。
	if err := l.Commit(slow, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append([]byte("retry")); err != nil {
		t.Fatalf("append after consumer commit: %v", err)
	}
	if first := l.FirstOffset(); first != 8 {
		t.Fatalf("first offset after auto-reclaim: got %d want 8", first)
	}
	if _, err := l.Read(0); !errors.Is(err, ErrOffsetReclaimed) {
		t.Fatalf("read reclaimed: want ErrOffsetReclaimed, got %v", err)
	}
	rec, err := l.Read(8)
	if err != nil {
		t.Fatalf("read live offset 8: %v", err)
	}
	if string(rec) != "r08" {
		t.Fatalf("record 8 mismatch: %q", rec)
	}
}

// 段数量上限：段数耗尽且无法回收时写入报 ErrNoCapacity，消费推进后恢复。
func TestCapacityMaxSegments(t *testing.T) {
	dir := t.TempDir()
	cfg := splitTestConfig(dir)
	cfg.MaxSegments = 3
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if _, err := l.RegisterConsumer(0); err != nil {
		t.Fatal(err)
	}
	var lastErr error
	var n int
	for ; n < 100; n++ {
		if _, err := l.Append([]byte(fmt.Sprintf("r%02d", n))); err != nil {
			lastErr = err
			break
		}
	}
	if !errors.Is(lastErr, ErrNoCapacity) {
		t.Fatalf("want ErrNoCapacity, got %v (appended %d)", lastErr, n)
	}
	if got := len(segFileNames(t, dir)); got > 3 {
		t.Fatalf("segment count exceeded cap: %d", got)
	}
	t.Logf("segment cap hit after %d records, classified as ErrNoCapacity", n)
}

// 文件句柄上限：段再多，同时打开的句柄也不超过配置。
func TestCapacityMaxOpenFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := splitTestConfig(dir)
	cfg.MaxOpenFiles = 2
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	const n = 40 // 约 10 个段
	for i := 0; i < n; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("r%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		rec, err := l.Read(uint64(i))
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if string(rec) != fmt.Sprintf("r%02d", i) {
			t.Fatalf("record %d mismatch: %q", i, rec)
		}
	}
	l.handles.mu.Lock()
	open := l.handles.open
	l.handles.mu.Unlock()
	t.Logf("open handles with cap=2: %d", open)
	if open > 2 {
		t.Fatalf("open handles %d exceed cap 2", open)
	}
}

// 回收跟着消费进度走：慢消费者没越过的数据不会被回收。
func TestConsumerGatedReclaim(t *testing.T) {
	dir := t.TempDir()
	cfg := splitTestConfig(dir)
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	const n = 20 // 4 条/段，共 5 段
	for i := 0; i < n; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("r%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	slow, err := l.RegisterConsumer(3)
	if err != nil {
		t.Fatal(err)
	}
	fast, err := l.RegisterConsumer(15)
	if err != nil {
		t.Fatal(err)
	}
	// 地平线 = min(3, 15) = 3：Reclaim(100) 也不能越过慢消费者。
	if got := l.ReclaimableOffset(); got != 3 {
		t.Fatalf("reclaimable horizon: got %d want 3", got)
	}
	if err := l.Reclaim(100); err != nil {
		t.Fatal(err)
	}
	if first := l.FirstOffset(); first != 0 {
		t.Fatalf("slow consumer pinned data reclaimed: first=%d", first)
	}
	// 慢消费者推进到 9：前两段（end=4、8）可回收。
	if err := l.Commit(slow, 9); err != nil {
		t.Fatal(err)
	}
	if err := l.Reclaim(100); err != nil {
		t.Fatal(err)
	}
	if first := l.FirstOffset(); first != 8 {
		t.Fatalf("first offset: got %d want 8", first)
	}
	if _, err := l.Read(7); !errors.Is(err, ErrOffsetReclaimed) {
		t.Fatalf("read reclaimed: want ErrOffsetReclaimed, got %v", err)
	}
	if _, err := l.Read(20); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("read unwritten: want ErrOffsetOutOfRange, got %v", err)
	}
	rec, err := l.Read(9)
	if err != nil {
		t.Fatal(err)
	}
	if string(rec) != "r09" {
		t.Fatalf("record 9 mismatch: %q", rec)
	}
	// 提交进度只前进不回退。
	if err := l.Commit(slow, 2); err != nil {
		t.Fatal(err)
	}
	if got := l.ReclaimableOffset(); got != 9 {
		t.Fatalf("horizon regressed: got %d want 9", got)
	}
	if err := l.Commit(9999, 1); !errors.Is(err, ErrUnknownConsumer) {
		t.Fatalf("unknown consumer: want ErrUnknownConsumer, got %v", err)
	}
	// 注销慢消费者后，地平线由 fast（15）决定。
	if err := l.UnregisterConsumer(slow); err != nil {
		t.Fatal(err)
	}
	if got := l.ReclaimableOffset(); got != 15 {
		t.Fatalf("horizon after unregister: got %d want 15", got)
	}
	// 全部注销后，地平线回到最新位置。
	if err := l.UnregisterConsumer(fast); err != nil {
		t.Fatal(err)
	}
	if got := l.ReclaimableOffset(); got != n {
		t.Fatalf("horizon with no consumers: got %d want %d", got, n)
	}
}

// 跨段记录的首分片段被回收、续段保留时：被续写的记录判定为已回收，
// 续段中起始的后续记录仍可读；重启后结论一致。
func TestSplitRecordReclaimContinuation(t *testing.T) {
	dir := t.TempDir()
	cfg := splitTestConfig(dir)
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append([]byte("a")); err != nil { // off 0
		t.Fatal(err)
	}
	big := make([]byte, 120) // 拆成 3 个分片（51+51+18）
	for i := range big {
		big[i] = byte(i)
	}
	if _, err := l.Append(big); err != nil { // off 1，跨段
		t.Fatal(err)
	}
	for _, s := range []string{"b", "c", "d"} { // off 2,3,4；b、c 落在续段里
		if _, err := l.Append([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	cons, err := l.RegisterConsumer(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Commit(cons, 2); err != nil { // 已消费记录 0、1
		t.Fatal(err)
	}
	if err := l.Reclaim(100); err != nil {
		t.Fatal(err)
	}
	if first := l.FirstOffset(); first != 2 {
		t.Fatalf("first offset: got %d want 2", first)
	}
	if _, err := l.Read(1); !errors.Is(err, ErrOffsetReclaimed) {
		t.Fatalf("read reclaimed split record: want ErrOffsetReclaimed, got %v", err)
	}
	for i, want := range []string{"b", "c", "d"} {
		rec, err := l.Read(uint64(2 + i))
		if err != nil {
			t.Fatalf("read %d from continuation segment: %v", 2+i, err)
		}
		if string(rec) != want {
			t.Fatalf("record %d mismatch: %q want %q", 2+i, rec, want)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// 重启：续段开头的残余分片被跳过，结论一致。
	l2, info, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	t.Logf("recovery with reclaimed continuation: next=%d validEnd=%d corrupt=%s",
		info.NextOffset, info.ValidEnd, info.Corrupt)
	if info.Corrupt != CorruptNone {
		t.Fatalf("unexpected corruption: %s", info.Corrupt)
	}
	if info.NextOffset != 5 {
		t.Fatalf("next offset: got %d want 5", info.NextOffset)
	}
	if first := l2.FirstOffset(); first != 2 {
		t.Fatalf("first offset after reopen: got %d want 2", first)
	}
	rec, err := l2.Read(2)
	if err != nil {
		t.Fatal(err)
	}
	if string(rec) != "b" {
		t.Fatalf("record 2 mismatch after reopen: %q", rec)
	}
}

// 并发：写（含跨段大记录）、读、消费进度上报、回收、刷盘同时进行。
func TestConcurrentCapacityReclaim(t *testing.T) {
	dir := t.TempDir()
	cfg := splitTestConfig(dir)
	cfg.MaxSegmentBytes = 128
	cfg.MaxRecordBytes = 4096
	cfg.MaxOpenFiles = 4 // 句柄上限下并发读写
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	cons, err := l.RegisterConsumer(0)
	if err != nil {
		t.Fatal(err)
	}
	var writers sync.WaitGroup
	var bg sync.WaitGroup
	stop := make(chan struct{})

	// 写：小记录为主，穿插跨段大记录。
	for w := 0; w < 4; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			for i := 0; i < 60; i++ {
				var payload []byte
				if i%10 == 9 {
					payload = make([]byte, 300) // 跨段
					for j := range payload {
						payload[j] = byte(w + j)
					}
				} else {
					payload = []byte(fmt.Sprintf("w%d-%d", w, i))
				}
				if _, err := l.Append(payload); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(w)
	}
	// 读：只读已提交范围，容忍并发回收。
	bg.Add(1)
	go func() {
		defer bg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			first, next := l.FirstOffset(), l.NextOffset()
			for off := first; off < next; off++ {
				_, err := l.Read(off)
				if err == nil || errors.Is(err, ErrOffsetReclaimed) {
					continue
				}
				t.Errorf("read %d: %v", off, err)
				return
			}
		}
	}()
	// 消费进度上报 + 回收 + 刷盘。
	bg.Add(1)
	go func() {
		defer bg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			next := l.NextOffset()
			if next > 4 {
				if err := l.Commit(cons, next-4); err != nil {
					t.Errorf("commit: %v", err)
					return
				}
			}
			if err := l.Reclaim(l.ReclaimableOffset()); err != nil {
				t.Errorf("reclaim: %v", err)
				return
			}
			if err := l.Sync(); err != nil {
				t.Errorf("sync: %v", err)
				return
			}
		}
	}()
	writers.Wait()
	close(stop)
	bg.Wait()

	l.handles.mu.Lock()
	open := l.handles.open
	l.handles.mu.Unlock()
	t.Logf("open handles after concurrent run (cap=4): %d", open)
	if open > 4 {
		t.Fatalf("open handles %d exceed cap 4", open)
	}
}
