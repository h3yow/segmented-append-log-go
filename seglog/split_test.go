package seglog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// splitTestConfig 小段尺寸，便于触发跨段拆分。
func splitTestConfig(dir string) Config {
	cfg := testConfig(dir)
	cfg.MaxSegmentBytes = 64
	cfg.MaxRecordBytes = 1 << 20
	return cfg
}

func writeSegFile(t *testing.T, dir string, base uint64, part uint16, data []byte) {
	t.Helper()
	name := segmentFileName(base, part)
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func segFileNames(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".seg") {
			names = append(names, e.Name())
		}
	}
	return names
}

func TestSplitRecordRoundtrip(t *testing.T) {
	dir := t.TempDir()
	cfg := splitTestConfig(dir)
	l, _, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// 3 条小记录（每条 14 字节）。
	for i := 0; i < 3; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("s%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// 一条 200 字节记录：编码后 212 字节 > 64 字节段上限，拆成多个分片。
	big := make([]byte, 200)
	for i := range big {
		big[i] = byte(i * 7)
	}
	off, err := l.Append(big)
	if err != nil {
		t.Fatal(err)
	}
	if off != 3 {
		t.Fatalf("split record offset: got %d want 3", off)
	}
	// 再写 3 条小记录，偏移应连续。
	for i := 0; i < 3; i++ {
		o, err := l.Append([]byte(fmt.Sprintf("t%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if o != uint64(4+i) {
			t.Fatalf("offset after split: got %d want %d", o, 4+i)
		}
	}
	names := segFileNames(t, dir)
	hasPart := false
	for _, n := range names {
		if strings.Contains(n, "-") {
			hasPart = true
		}
	}
	if !hasPart {
		t.Fatalf("expected continuation segment files, got %v", names)
	}
	t.Logf("segment files: %v", names)

	check := func(l *Log) {
		t.Helper()
		for i := 0; i < 3; i++ {
			rec, err := l.Read(uint64(i))
			if err != nil {
				t.Fatalf("read small %d: %v", i, err)
			}
			if string(rec) != fmt.Sprintf("s%d", i) {
				t.Fatalf("small record %d mismatch: %q", i, rec)
			}
		}
		rec, err := l.Read(3)
		if err != nil {
			t.Fatalf("read split record: %v", err)
		}
		if len(rec) != len(big) {
			t.Fatalf("split record length: got %d want %d", len(rec), len(big))
		}
		for i := range big {
			if rec[i] != big[i] {
				t.Fatalf("split record byte %d: got %d want %d", i, rec[i], big[i])
			}
		}
		for i := 0; i < 3; i++ {
			rec, err := l.Read(uint64(4 + i))
			if err != nil {
				t.Fatalf("read tail %d: %v", i, err)
			}
			if string(rec) != fmt.Sprintf("t%d", i) {
				t.Fatalf("tail record %d mismatch: %q", i, rec)
			}
		}
	}
	check(l)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// 重开后跨段记录仍应作为一条完整记录读回。
	l2, info, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	t.Logf("recovery after reopen: next=%d validEnd=%d corrupt=%s",
		info.NextOffset, info.ValidEnd, info.Corrupt)
	if info.Corrupt != CorruptNone {
		t.Fatalf("unexpected corruption: %s", info.Corrupt)
	}
	if info.NextOffset != 7 {
		t.Fatalf("next offset after reopen: got %d want 7", info.NextOffset)
	}
	check(l2)
}

// 模拟跨段记录在不同写入阶段崩溃，验证恢复结论与可继续写入位置。
func TestSplitRecordCrashStages(t *testing.T) {
	chunkCap := 64 - fragHdrSize // 51
	chunk := func(seed, n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(seed + i)
		}
		return b
	}
	first := encodeFragment(fragFirst, chunk(1, chunkCap))
	middle := encodeFragment(0, chunk(2, chunkCap))
	last := encodeFragment(fragLast, chunk(3, 20))

	cases := []struct {
		name  string
		files map[[2]uint64][]byte // [base, part] -> 内容（模拟崩溃时各段已写入的字节）
		want  CorruptKind
		next  uint64
	}{
		{"first-fragment-only", map[[2]uint64][]byte{
			{5, 0}: first,
		}, CorruptPartialRecord, 5},
		{"partial-second-fragment-header", map[[2]uint64][]byte{
			{5, 0}: first,
			{5, 1}: middle[:7],
		}, CorruptPartialRecord, 5},
		{"partial-second-fragment-payload", map[[2]uint64][]byte{
			{5, 0}: first,
			{5, 1}: middle[:fragHdrSize+10],
		}, CorruptPartialRecord, 5},
		{"middle-complete-no-last", map[[2]uint64][]byte{
			{5, 0}: first,
			{5, 1}: middle,
		}, CorruptPartialRecord, 5},
		{"partial-first-fragment-header", map[[2]uint64][]byte{
			{5, 0}: first[:7],
		}, CorruptTornTail, 5},
		{"bad-crc-first-fragment", map[[2]uint64][]byte{
			{5, 0}: corruptCRC(first),
		}, CorruptChecksum, 5},
		{"all-fragments-written", map[[2]uint64][]byte{
			{5, 0}: first,
			{5, 1}: middle,
			{5, 2}: last,
		}, CorruptNone, 6},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := splitTestConfig(dir)
			l, _, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 5; i++ {
				if _, err := l.Append([]byte(fmt.Sprintf("g%d", i))); err != nil {
					t.Fatal(err)
				}
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			// 模拟崩溃：跨段记录写到一半，各段只留下已写入的字节。
			for k, data := range tc.files {
				writeSegFile(t, dir, k[0], uint16(k[1]), data)
			}

			l2, info, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer l2.Close()
			t.Logf("recovery: next=%d validEnd=%d truncated=%d corrupt=%s",
				info.NextOffset, info.ValidEnd, info.TruncatedByte, info.Corrupt)
			if info.Corrupt != tc.want {
				t.Fatalf("corrupt kind: got %s want %s", info.Corrupt, tc.want)
			}
			if info.NextOffset != tc.next || info.ValidEnd != tc.next {
				t.Fatalf("valid range: next=%d validEnd=%d, want %d/%d",
					info.NextOffset, info.ValidEnd, tc.next, tc.next)
			}
			// 好记录完整可读。
			for i := 0; i < 5; i++ {
				rec, err := l2.Read(uint64(i))
				if err != nil {
					t.Fatalf("read good %d: %v", i, err)
				}
				if string(rec) != fmt.Sprintf("g%d", i) {
					t.Fatalf("good record %d mismatch: %q", i, rec)
				}
			}
			if tc.want == CorruptNone {
				// 完整跨段记录：作为一条记录读回，内容正确。
				rec, err := l2.Read(5)
				if err != nil {
					t.Fatalf("read split record: %v", err)
				}
				want := append(append(chunk(1, chunkCap), chunk(2, chunkCap)...), chunk(3, 20)...)
				if len(rec) != len(want) {
					t.Fatalf("split record length: got %d want %d", len(rec), len(want))
				}
				for i := range want {
					if rec[i] != want[i] {
						t.Fatalf("split record byte %d mismatch", i)
					}
				}
			} else {
				// 写到一半的残余：不能作为完整记录读出，且不得残留续段文件。
				if _, err := l2.Read(5); !errors.Is(err, ErrOffsetOutOfRange) {
					t.Fatalf("read partial record: want ErrOffsetOutOfRange, got %v", err)
				}
				for _, n := range segFileNames(t, dir) {
					if strings.Contains(n, "-") {
						t.Fatalf("continuation segment not cleaned: %s", n)
					}
				}
			}
			// 恢复后可继续追加，偏移与正常关闭一致。
			off, err := l2.Append([]byte("after"))
			if err != nil {
				t.Fatal(err)
			}
			if off != tc.next {
				t.Fatalf("append after recovery: got %d want %d", off, tc.next)
			}
		})
	}
}

// 跨段记录场景下，正常关闭与异常终止（未 Close）的恢复结论一致。
func TestSplitRecordCleanVsCrashConsistency(t *testing.T) {
	build := func(dir string, clean bool) {
		cfg := splitTestConfig(dir)
		l, _, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 4; i++ {
			if _, err := l.Append([]byte(fmt.Sprintf("r%d", i))); err != nil {
				t.Fatal(err)
			}
		}
		big := make([]byte, 150) // 触发拆分
		if _, err := l.Append(big); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			if _, err := l.Append([]byte(fmt.Sprintf("z%d", i))); err != nil {
				t.Fatal(err)
			}
		}
		if clean {
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	dirClean, dirCrash := t.TempDir(), t.TempDir()
	build(dirClean, true)
	build(dirCrash, false)

	_, infoClean, err := Open(splitTestConfig(dirClean))
	if err != nil {
		t.Fatal(err)
	}
	_, infoCrash, err := Open(splitTestConfig(dirCrash))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("clean recovery: next=%d corrupt=%s", infoClean.NextOffset, infoClean.Corrupt)
	t.Logf("crash recovery: next=%d corrupt=%s", infoCrash.NextOffset, infoCrash.Corrupt)
	if *infoClean != *infoCrash {
		t.Fatalf("inconsistent recovery: clean=%+v crash=%+v", infoClean, infoCrash)
	}
	if infoClean.NextOffset != 8 {
		t.Fatalf("next offset: got %d want 8", infoClean.NextOffset)
	}
}

// 未刷盘崩溃：SyncOnAppend=false、不 Sync、不 Close 直接丢弃，
// 恢复结论应与正常关闭一致（数据已通过 WriteAt 到达操作系统）。
func TestUnsyncedCrashConsistency(t *testing.T) {
	build := func(dir string, clean bool) {
		cfg := splitTestConfig(dir)
		cfg.SyncOnAppend = false
		l, _, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			if _, err := l.Append([]byte(fmt.Sprintf("u%d", i))); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := l.Append(make([]byte, 150)); err != nil { // 跨段记录
			t.Fatal(err)
		}
		if clean {
			if err := l.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
		}
		// 异常终止：不 Sync 不 Close，直接放弃引用。
	}
	dirClean, dirCrash := t.TempDir(), t.TempDir()
	build(dirClean, true)
	build(dirCrash, false)

	_, infoClean, err := Open(splitTestConfig(dirClean))
	if err != nil {
		t.Fatal(err)
	}
	_, infoCrash, err := Open(splitTestConfig(dirCrash))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("clean recovery: next=%d corrupt=%s", infoClean.NextOffset, infoClean.Corrupt)
	t.Logf("unsynced crash recovery: next=%d corrupt=%s", infoCrash.NextOffset, infoCrash.Corrupt)
	if *infoClean != *infoCrash {
		t.Fatalf("inconsistent recovery: clean=%+v crash=%+v", infoClean, infoCrash)
	}
	if infoClean.NextOffset != 4 {
		t.Fatalf("next offset: got %d want 4", infoClean.NextOffset)
	}
}
