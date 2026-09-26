package seglog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func testConfig(dir string) Config {
	return Config{
		Dir:              dir,
		MaxSegmentBytes:  1 << 20,
		MaxRecordBytes:   1 << 16,
		MaxInflightBytes: 1 << 20,
		MaxReaders:       64,
		SyncOnAppend:     true,
	}
}

func TestAppendReadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	l, info, err := Open(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("recovery: next=%d validEnd=%d truncated=%d corrupt=%s",
		info.NextOffset, info.ValidEnd, info.TruncatedByte, info.Corrupt)
	const n = 100
	for i := 0; i < n; i++ {
		off, err := l.Append([]byte(fmt.Sprintf("record-%04d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if off != uint64(i) {
			t.Fatalf("offset not sequential: got %d want %d", off, i)
		}
	}
	for i := 0; i < n; i++ {
		rec, err := l.Read(uint64(i))
		if err != nil {
			t.Fatal(err)
		}
		if string(rec) != fmt.Sprintf("record-%04d", i) {
			t.Fatalf("record %d mismatch: %q", i, rec)
		}
	}
	if _, err := l.Read(n); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("read past end: want ErrOffsetOutOfRange, got %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

// 在最后一个段文件尾部追加残余字节，模拟写入中途崩溃。
func appendGarbage(t *testing.T, dir string, garbage []byte) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var last string
	for _, e := range ents {
		if filepath.Ext(e.Name()) == ".seg" && e.Name() > last {
			last = e.Name()
		}
	}
	f, err := os.OpenFile(filepath.Join(dir, last), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(garbage); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func TestRecoverTornTail(t *testing.T) {
	cases := []struct {
		name    string
		garbage []byte
		want    CorruptKind
	}{
		{"partial-header", []byte{0x31, 0x4C}, CorruptTornTail},
		{"partial-payload", encodeRecord([]byte("hello-world"))[:16], CorruptTornTail},
		{"bad-magic", []byte{0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0, 0, 0, 0, 0}, CorruptBadMagic},
		{"bad-length", []byte{0x31, 0x4C, 0x51, 0x53, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0}, CorruptBadLength},
		{"checksum", corruptCRC(encodeRecord([]byte("payload-data"))), CorruptChecksum},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			l, _, err := Open(testConfig(dir))
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 10; i++ {
				if _, err := l.Append([]byte(fmt.Sprintf("good-%d", i))); err != nil {
					t.Fatal(err)
				}
			}
			l.Close()
			appendGarbage(t, dir, tc.garbage)

			l2, info, err := Open(testConfig(dir))
			if err != nil {
				t.Fatal(err)
			}
			defer l2.Close()
			t.Logf("recovery: next=%d validEnd=%d truncated=%d corrupt=%s",
				info.NextOffset, info.ValidEnd, info.TruncatedByte, info.Corrupt)
			if info.Corrupt != tc.want {
				t.Fatalf("corrupt kind: got %s want %s", info.Corrupt, tc.want)
			}
			if info.NextOffset != 10 || info.ValidEnd != 10 {
				t.Fatalf("valid range: next=%d validEnd=%d, want 10/10", info.NextOffset, info.ValidEnd)
			}
			if info.TruncatedByte != int64(len(tc.garbage)) {
				t.Fatalf("truncated bytes: got %d want %d", info.TruncatedByte, len(tc.garbage))
			}
			// 旧数据完整可读，残余不作为记录返回。
			for i := 0; i < 10; i++ {
				rec, err := l2.Read(uint64(i))
				if err != nil {
					t.Fatal(err)
				}
				if string(rec) != fmt.Sprintf("good-%d", i) {
					t.Fatalf("record %d mismatch: %q", i, rec)
				}
			}
			// 恢复后可继续追加，偏移连续。
			off, err := l2.Append([]byte("after-recovery"))
			if err != nil {
				t.Fatal(err)
			}
			if off != 10 {
				t.Fatalf("append after recovery: got offset %d want 10", off)
			}
		})
	}
}

func corruptCRC(rec []byte) []byte {
	out := make([]byte, len(rec))
	copy(out, rec)
	out[8] ^= 0xFF
	return out
}

// 正常关闭与异常终止（不 Close 直接丢弃）对同一份数据的恢复结论一致。
func TestCleanShutdownVsCrashConsistency(t *testing.T) {
	build := func(dir string, clean bool) {
		l, _, err := Open(testConfig(dir))
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 50; i++ {
			if _, err := l.Append([]byte(fmt.Sprintf("rec-%d", i))); err != nil {
				t.Fatal(err)
			}
		}
		if clean {
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
		}
		// 异常终止：不 Close，直接放弃引用（数据已通过 WriteAt 落盘）。
	}
	dirClean, dirCrash := t.TempDir(), t.TempDir()
	build(dirClean, true)
	build(dirCrash, false)

	_, infoClean, err := Open(testConfig(dirClean))
	if err != nil {
		t.Fatal(err)
	}
	_, infoCrash, err := Open(testConfig(dirCrash))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("clean recovery: next=%d corrupt=%s", infoClean.NextOffset, infoClean.Corrupt)
	t.Logf("crash recovery: next=%d corrupt=%s", infoCrash.NextOffset, infoCrash.Corrupt)
	if *infoClean != *infoCrash {
		t.Fatalf("inconsistent recovery: clean=%+v crash=%+v", infoClean, infoCrash)
	}
}

func TestSegmentRotationAndReclaim(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxSegmentBytes = 128 // 强制快速滚动
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	const n = 40
	for i := 0; i < n; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("payload-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	ents, _ := os.ReadDir(dir)
	segCount := 0
	for _, e := range ents {
		if filepath.Ext(e.Name()) == ".seg" {
			segCount++
		}
	}
	if segCount < 2 {
		t.Fatalf("expected segment rotation, got %d segment(s)", segCount)
	}
	t.Logf("segments after rotation: %d", segCount)

	// 回收前一半数据。
	if err := l.Reclaim(n / 2); err != nil {
		t.Fatal(err)
	}
	first := l.FirstOffset()
	t.Logf("first offset after reclaim: %d", first)
	if first == 0 || first > n/2 {
		t.Fatalf("unexpected first offset %d", first)
	}
	if _, err := l.Read(0); !errors.Is(err, ErrOffsetReclaimed) {
		t.Fatalf("read reclaimed: want ErrOffsetReclaimed, got %v", err)
	}
	// 仍被引用的位置数据完整。
	for i := first; i < n; i++ {
		rec, err := l.Read(i)
		if err != nil {
			t.Fatalf("read live offset %d: %v", i, err)
		}
		if string(rec) != fmt.Sprintf("payload-%03d", i) {
			t.Fatalf("record %d mismatch: %q", i, rec)
		}
	}
	// 回收后重启，结论保持。
	l.Close()
	l2, info, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	t.Logf("recovery after reclaim: next=%d validEnd=%d corrupt=%s",
		info.NextOffset, info.ValidEnd, info.Corrupt)
	if l2.FirstOffset() != first {
		t.Fatalf("first offset changed after reopen: %d -> %d", first, l2.FirstOffset())
	}
	if l2.NextOffset() != n {
		t.Fatalf("next offset after reopen: got %d want %d", l2.NextOffset(), n)
	}
}

func TestConcurrentAppendRead(t *testing.T) {
	dir := t.TempDir()
	l, _, err := Open(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	const writers = 8
	const perWriter = 50
	var wg sync.WaitGroup
	offs := make([][]uint64, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				off, err := l.Append([]byte(fmt.Sprintf("w%d-%d", w, i)))
				if err != nil {
					t.Error(err)
					return
				}
				offs[w] = append(offs[w], off)
			}
		}(w)
	}
	// 并发读取已提交位置。
	stop := make(chan struct{})
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			next := l.NextOffset()
			for i := uint64(0); i < next; i++ {
				if _, err := l.Read(i); err != nil {
					t.Errorf("read committed offset %d: %v", i, err)
					return
				}
			}
		}
	}()
	wg.Wait()
	close(stop)
	readerWG.Wait()

	// 偏移全局唯一且覆盖 [0, writers*perWriter)。
	seen := make(map[uint64]bool)
	for _, list := range offs {
		if len(list) != perWriter {
			t.Fatalf("writer lost appends: %d/%d", len(list), perWriter)
		}
		for _, o := range list {
			if seen[o] {
				t.Fatalf("duplicate offset %d", o)
			}
			seen[o] = true
		}
	}
	for i := uint64(0); i < writers*perWriter; i++ {
		if !seen[i] {
			t.Fatalf("missing offset %d", i)
		}
	}
}

func TestBackpressureAndLimits(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.SyncOnAppend = false
	cfg.MaxInflightBytes = 64
	cfg.MaxRecordBytes = 32
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if _, err := l.Append(make([]byte, 33)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("want ErrRecordTooLarge, got %v", err)
	}
	// 每条记录 = 12 字节头 + 20 字节负载 = 32 字节；两条即达到 64 上限。
	if _, err := l.Append(make([]byte, 20)); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(make([]byte, 20)); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(make([]byte, 20)); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("want ErrBackpressure, got %v", err)
	}
	t.Logf("backpressure triggered at inflight limit, classified as ErrBackpressure")
	// 刷盘后恢复可写。
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(make([]byte, 20)); err != nil {
		t.Fatalf("append after sync: %v", err)
	}
}

func TestCrashMidSegmentThenReopen(t *testing.T) {
	// 模拟崩溃发生在段滚动之后、新段只写了半个头的场景。
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxSegmentBytes = 64
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	next := l.NextOffset()
	l.Close()
	appendGarbage(t, dir, []byte{0x31, 0x4C, 0x51}) // 半个头

	l2, info, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	t.Logf("recovery: next=%d validEnd=%d truncated=%d corrupt=%s",
		info.NextOffset, info.ValidEnd, info.TruncatedByte, info.Corrupt)
	if info.Corrupt != CorruptTornTail {
		t.Fatalf("got %s want torn-tail", info.Corrupt)
	}
	if info.NextOffset != next {
		t.Fatalf("next offset: got %d want %d", info.NextOffset, next)
	}
	for i := uint64(0); i < next; i++ {
		rec, err := l2.Read(i)
		if err != nil {
			t.Fatal(err)
		}
		if string(rec) != fmt.Sprintf("v%d", i) {
			t.Fatalf("record %d mismatch: %q", i, rec)
		}
	}
}
