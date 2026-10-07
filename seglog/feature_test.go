package seglog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// 小容量配置：段 64 字节，便于制造跨段记录与容量压力。
func smallConfig(dir string) Config {
	return Config{
		Dir:              dir,
		MaxSegmentBytes:  64,
		MaxRecordBytes:   1 << 16,
		MaxInflightBytes: 1 << 20,
		MaxReaders:       64,
		SyncOnAppend:     true,
		MaxTotalBytes:    1 << 20,
		MaxSegments:      64,
		MaxOpenFiles:     8,
	}
}

func logRecovery(t *testing.T, tag string, info *RecoveryInfo) {
	t.Helper()
	t.Logf("[%s] recovery: next=%d validEnd=%d truncated=%d corrupt=%s",
		tag, info.NextOffset, info.ValidEnd, info.TruncatedByte, info.Corrupt)
}

// 跨段记录：大于单段的记录被拆成连续多块，读回仍是完整一条。
func TestChunkedRecordRoundtrip(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig(dir)
	l, info, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	logRecovery(t, "open", info)

	// 3 条小记录（各 14 字节）+ 1 条 100 字节大记录（跨 3 段）。
	for i := 0; i < 3; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("r%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	big := make([]byte, 100)
	for i := range big {
		big[i] = byte('A' + i%26)
	}
	off, err := l.Append(big)
	if err != nil {
		t.Fatal(err)
	}
	if off != 3 {
		t.Fatalf("big record offset: got %d want 3", off)
	}
	segCount := 0
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if filepath.Ext(e.Name()) == ".seg" {
			segCount++
		}
	}
	t.Logf("segments after chunked append: %d", segCount)
	if segCount < 3 {
		t.Fatalf("expected record to span multiple segments, got %d segment(s)", segCount)
	}
	// 大记录之后偏移继续连续。
	off, err = l.Append([]byte("tail"))
	if err != nil {
		t.Fatal(err)
	}
	if off != 4 {
		t.Fatalf("offset after big record: got %d want 4", off)
	}

	got, err := l.Read(3)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(big) {
		t.Fatalf("chunked record mismatch: got %d bytes want %d", len(got), len(big))
	}
	l.Close()

	// 重启后跨段记录仍完整可读。
	l2, info2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	logRecovery(t, "reopen", info2)
	if info2.Corrupt != CorruptNone {
		t.Fatalf("unexpected corrupt kind after clean close: %s", info2.Corrupt)
	}
	got, err = l2.Read(3)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(big) {
		t.Fatalf("chunked record mismatch after reopen: got %d bytes", len(got))
	}
	if rec, err := l2.Read(4); err != nil || string(rec) != "tail" {
		t.Fatalf("record after chunked: %q %v", rec, err)
	}
}

// 在指定段文件尾部追加字节，模拟写到一半崩溃留下的残余。
func appendToSegment(t *testing.T, dir string, base uint64, data []byte) {
	t.Helper()
	path := filepath.Join(dir, segmentFileName(base, 0))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

// 模拟跨段大记录在不同写入阶段崩溃，验证恢复范围与可继续写入位置
// 与“只写了 3 条小记录后正常关闭”一致。
func TestCrashDuringChunkedRecord(t *testing.T) {
	// 布局：段 64 字节；3 条小记录（各 14B）占 42B；
	// 大记录 100B 分块：First(10B 负载) 填满段0，Middle(52B) 在段1，Last(38B) 在段1。
	big := make([]byte, 100)
	for i := range big {
		big[i] = byte('a' + i%26)
	}
	first := encodeChunk(magicChunkFirst, big[:10])     // 22B，段0 收尾
	middle := encodeChunk(magicChunkMiddle, big[10:62]) // 64B，段1
	last := encodeChunk(magicChunkLast, big[62:])       // 50B，段1

	cases := []struct {
		name  string
		stage func(t *testing.T, dir string)
		want  CorruptKind
	}{
		{"first-chunk-header-partial", func(t *testing.T, dir string) {
			appendToSegment(t, dir, 0, first[:5]) // 块头只写了一半
		}, CorruptTornTail},
		{"first-chunk-complete-only", func(t *testing.T, dir string) {
			appendToSegment(t, dir, 0, first) // 首块完整落盘后崩溃
		}, CorruptPartialRecord},
		{"middle-chunk-partial-payload", func(t *testing.T, dir string) {
			appendToSegment(t, dir, 0, first)
			appendToSegment(t, dir, 4, middle[:30]) // 中间块负载写了一半
		}, CorruptPartialRecord},
		{"middle-chunk-complete", func(t *testing.T, dir string) {
			appendToSegment(t, dir, 0, first)
			appendToSegment(t, dir, 4, middle) // 中间块完整、末块未写
		}, CorruptPartialRecord},
		{"last-chunk-header-partial", func(t *testing.T, dir string) {
			appendToSegment(t, dir, 0, first)
			appendToSegment(t, dir, 4, middle)
			appendToSegment(t, dir, 4, last[:7]) // 末块头写了一半（落盘到一半）
		}, CorruptPartialRecord},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := smallConfig(dir)
			l, _, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				if _, err := l.Append([]byte(fmt.Sprintf("r%d", i))); err != nil {
					t.Fatal(err)
				}
			}
			l.Close()
			tc.stage(t, dir)

			l2, info, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer l2.Close()
			logRecovery(t, tc.name, info)
			if info.Corrupt != tc.want {
				t.Fatalf("corrupt kind: got %s want %s", info.Corrupt, tc.want)
			}
			// 可用范围与“只写 3 条小记录后正常关闭”一致。
			if info.NextOffset != 3 || info.ValidEnd != 3 {
				t.Fatalf("valid range: next=%d validEnd=%d, want 3/3", info.NextOffset, info.ValidEnd)
			}
			if info.TruncatedByte <= 0 {
				t.Fatalf("expected residue truncated, got %d", info.TruncatedByte)
			}
			for i := 0; i < 3; i++ {
				rec, err := l2.Read(uint64(i))
				if err != nil {
					t.Fatal(err)
				}
				if string(rec) != fmt.Sprintf("r%d", i) {
					t.Fatalf("record %d mismatch: %q", i, rec)
				}
			}
			// 半截记录不作为完整记录读出。
			if _, err := l2.Read(3); !errors.Is(err, ErrOffsetOutOfRange) {
				t.Fatalf("partial record visible: want ErrOffsetOutOfRange, got %v", err)
			}
			// 恢复后可继续追加，偏移从 3 连续。
			off, err := l2.Append([]byte("after"))
			if err != nil {
				t.Fatal(err)
			}
			if off != 3 {
				t.Fatalf("append after recovery: got offset %d want 3", off)
			}
		})
	}
}

// 容量上限：总字节数触顶时先按消费进度自动回收，回收不出空间则返回
// 明确的 ErrNoCapacity（与 ErrBackpressure 可区分）。
func TestCapacityTotalBytes(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig(dir)
	cfg.MaxTotalBytes = 200 // 约 3 段
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	r, err := l.RegisterReader("consumer")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// 读取方不提交进度：什么都回收不了，写满即拒绝。
	var wrote int
	for {
		_, err := l.Append([]byte("payload-data"))
		if err != nil {
			if !errors.Is(err, ErrNoCapacity) {
				t.Fatalf("want ErrNoCapacity, got %v", err)
			}
			if errors.Is(err, ErrBackpressure) {
				t.Fatal("ErrNoCapacity must be distinguishable from ErrBackpressure")
			}
			break
		}
		wrote++
	}
	t.Logf("capacity reached after %d records, classified as ErrNoCapacity", wrote)
	if wrote == 0 {
		t.Fatal("no records written before capacity error")
	}

	// 提交消费进度后，追加触发自动回收腾出空间。
	if err := r.Commit(l.NextOffset()); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append([]byte("payload-data")); err != nil {
		t.Fatalf("append after commit should auto-reclaim: %v", err)
	}
	t.Logf("after commit, auto-reclaim freed space; first offset=%d", l.FirstOffset())
	if _, err := l.Read(0); !errors.Is(err, ErrOffsetReclaimed) {
		t.Fatalf("read reclaimed: want ErrOffsetReclaimed, got %v", err)
	}
}

// 段数量上限：触顶且回收不出空间时返回 ErrNoCapacity。
func TestCapacityMaxSegments(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig(dir)
	cfg.MaxSegments = 3
	cfg.MaxTotalBytes = 1 << 20
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	r, _ := l.RegisterReader("c")
	defer r.Close()

	var wrote int
	for {
		_, err := l.Append([]byte("seg-filler-record"))
		if err != nil {
			if !errors.Is(err, ErrNoCapacity) {
				t.Fatalf("want ErrNoCapacity, got %v", err)
			}
			break
		}
		wrote++
	}
	t.Logf("segment cap reached after %d records (MaxSegments=3)", wrote)

	if err := r.Commit(l.NextOffset()); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append([]byte("seg-filler-record")); err != nil {
		t.Fatalf("append after commit should auto-reclaim segments: %v", err)
	}
}

// 回收跟着真实消费进度走：慢消费者没越过的数据不能被回收。
func TestConsumerGatedReclaim(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig(dir)
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	const n = 40
	for i := 0; i < n; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("rec-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	slow, _ := l.RegisterReader("slow")
	fast, _ := l.RegisterReader("fast")
	defer slow.Close()
	defer fast.Close()

	if err := slow.Commit(5); err != nil {
		t.Fatal(err)
	}
	if err := fast.Commit(30); err != nil {
		t.Fatal(err)
	}
	// 请求回收到 30，但慢消费者只到 5：回收界被钳制到 5。
	if err := l.Reclaim(30); err != nil {
		t.Fatal(err)
	}
	first := l.FirstOffset()
	t.Logf("reclaim requested=30 clamped by slow consumer: first=%d", first)
	if first > 5 {
		t.Fatalf("reclaimed past slow consumer: first=%d", first)
	}
	// 慢消费者还没读到的数据仍在。
	for i := first; i < n; i++ {
		if _, err := l.Read(i); err != nil {
			t.Fatalf("read live offset %d: %v", i, err)
		}
	}
	// 慢消费者赶上来后可以回收更多。
	if err := slow.Commit(30); err != nil {
		t.Fatal(err)
	}
	if err := l.Reclaim(30); err != nil {
		t.Fatal(err)
	}
	first = l.FirstOffset()
	t.Logf("after slow caught up: first=%d", first)
	if first == 0 || first > 30 {
		t.Fatalf("unexpected first offset %d", first)
	}
	// 已回收位置与从未写过的位置可区分。
	if _, err := l.Read(0); !errors.Is(err, ErrOffsetReclaimed) {
		t.Fatalf("read reclaimed: want ErrOffsetReclaimed, got %v", err)
	}
	if _, err := l.Read(n); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("read never-written: want ErrOffsetOutOfRange, got %v", err)
	}
	// 注销读取方后不再钉住回收界。
	if err := slow.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fast.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Reclaim(n); err != nil {
		t.Fatal(err)
	}
	t.Logf("all readers closed, reclaim to %d: first=%d", n, l.FirstOffset())
}

// 文件句柄上限：段数远多于句柄上限时，读写正常且句柄数不越界。
func TestMaxOpenFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig(dir)
	cfg.MaxOpenFiles = 2
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	const n = 30
	for i := 0; i < n; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("rec-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	segCount := 0
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if filepath.Ext(e.Name()) == ".seg" {
			segCount++
		}
	}
	t.Logf("segments=%d, MaxOpenFiles=2", segCount)
	if segCount <= 2 {
		t.Fatalf("expected many segments, got %d", segCount)
	}
	// 跨所有段随机读，句柄池 LRU 淘汰，不越上限。
	for i := 0; i < n; i++ {
		rec, err := l.Read(uint64(n - 1 - i))
		if err != nil {
			t.Fatal(err)
		}
		if string(rec) != fmt.Sprintf("rec-%03d", n-1-i) {
			t.Fatalf("record %d mismatch: %q", n-1-i, rec)
		}
		if c := l.pool.openCount(); c > 2 {
			t.Fatalf("open files %d exceeds MaxOpenFiles=2", c)
		}
	}
	t.Logf("open file handles stayed within cap: %d", l.pool.openCount())
}

// 并发：写、读、消费进度提交、回收同时进行，竞态检测下应干净。
func TestConcurrentAppendReadCommitReclaim(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig(dir)
	cfg.MaxTotalBytes = 8 << 10
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	r, err := l.RegisterReader("consumer")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// 写方：容量满时 ErrNoCapacity 是合法结果，等回收后重试。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_, err := l.Append([]byte(fmt.Sprintf("msg-%06d", i)))
			if errors.Is(err, ErrNoCapacity) {
				continue
			}
			if err != nil {
				t.Errorf("append: %v", err)
				return
			}
		}
	}()
	// 消费方：读已提交位置并推进消费进度。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			next := l.NextOffset()
			first := l.FirstOffset()
			for i := first; i < next; i++ {
				if _, err := l.Read(i); err != nil && !errors.Is(err, ErrOffsetReclaimed) {
					t.Errorf("read %d: %v", i, err)
					return
				}
			}
			if err := r.Commit(next); err != nil {
				return
			}
		}
	}()
	// 回收方：按消费进度回收。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := l.Reclaim(l.NextOffset()); err != nil {
				t.Errorf("reclaim: %v", err)
				return
			}
		}
	}()

	// 跑一小段时间后收尾。
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	t.Logf("final: first=%d next=%d", l.FirstOffset(), l.NextOffset())
	if l.NextOffset() == 0 {
		t.Fatal("no records appended during concurrent run")
	}
}

// 崩溃发生在“刚滚动出新段、还没写入任何块”的窗口：空尾段应被识别并清除，
// 恢复位置与正常关闭一致。
func TestRecoverEmptyTailSegment(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig(dir)
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("r%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	next := l.NextOffset()
	l.Close()

	// 模拟崩溃窗口：滚动出的新段文件已创建但没有任何内容。
	empty, err := os.OpenFile(filepath.Join(dir, segmentFileName(next, 0)),
		os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	empty.Close()

	l2, info, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	logRecovery(t, "empty-tail-segment", info)
	if info.Corrupt != CorruptNone {
		t.Fatalf("empty tail segment should not be corruption, got %s", info.Corrupt)
	}
	if info.NextOffset != next {
		t.Fatalf("next: got %d want %d", info.NextOffset, next)
	}
	off, err := l2.Append([]byte("resume"))
	if err != nil {
		t.Fatal(err)
	}
	if off != next {
		t.Fatalf("append after recovery: got %d want %d", off, next)
	}
	for i := uint64(0); i < next; i++ {
		rec, err := l2.Read(i)
		if err != nil {
			t.Fatal(err)
		}
		if string(rec) != fmt.Sprintf("r%d", i) {
			t.Fatalf("record %d mismatch: %q", i, rec)
		}
	}
}
