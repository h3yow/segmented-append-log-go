package seglog

import (
	"errors"
	"io"
	"os"
	"sort"
)

// recovery.go：启动扫描与崩溃恢复。
//
// 恢复只依据磁盘字节推导，不依赖任何“正常关闭”标记：
//  1. 列出目录中所有合法分段文件，按 basePosition 排序；
//  2. 把所有分段视作一条连续字节流，顺序扫描帧并运行“记录分片链”
//     状态机，记录每一帧是否构成 完整 + CRC 正确 + 序号连续 的记录；
//  3. 第一个不满足条件的帧即为损坏点：该帧起到流末尾的全部字节都属
//     尾部残余；映射回各分段后，损坏点之后的分段整段删除，损坏点所在
//     分段截断（可能清空），之前的分段原样保留；
//  4. 因而正常关闭与异常终止对同一份磁盘数据得到完全一致的可用范围。

// segScan 累积单个分段在全局扫描中的状态。
type segScan struct {
	seg         *segment
	size        int64
	valid       int64 // 该分段内已确认属于某条完整记录的字节数
	firstSeq    uint64
	lastSeq     uint64
	haveRecords bool
}

// recoverDir 扫描目录并修复尾部，返回恢复结论。
func recoverDir(dir string, cfg Config) (*RecoveryInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type listed struct {
		base uint64
		name string
	}
	var files []listed
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if base, ok := parseSegmentName(e.Name()); ok {
			files = append(files, listed{base, e.Name()})
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].base < files[j].base })

	info := &RecoveryInfo{Corruption: CorruptionNone}
	if len(files) == 0 {
		return info, nil
	}

	// 打开全部文件并建立扫描上下文。
	scans := make([]*segScan, len(files))
	var globalOffset int64
	for i, f := range files {
		path := dir + "/" + f.name
		fi, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		scans[i] = &segScan{
			seg: &segment{
				path:         path,
				basePosition: f.base,
				startOffset:  globalOffset,
				size:         fi.Size(),
			},
			size: fi.Size(),
		}
		globalOffset += fi.Size()
	}

	// 全局记录状态机。
	var (
		lastSeq     uint64 // 最后一条已闭合记录序号
		haveRecords bool
		chainOpen   bool   // 存在未闭合的首帧->...链
		chainSeq    uint64 // 悬挂链记录序号
		chainSegIdx int    // 悬挂链首帧所在分段
		chainSegOff int64  // 悬挂链首帧在该分段内偏移
		bad         bool
		badKind     CorruptionKind
		badSegIdx   int
		badSegOff   int64
	)
	markBad := func(idx int, off int64, kind CorruptionKind) {
		if !bad {
			bad = true
			badKind = kind
			badSegIdx = idx
			badSegOff = off
		}
	}

	for idx, sc := range scans {
		fh, err := os.OpenFile(sc.seg.path, os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		fr := newFrameReader(fh)
		var off int64
	scanLoop:
		for {
			frameStart := off
			h, hdrEnd, herr := fr.nextHeader()
			if herr != nil {
				if errors.Is(herr, io.EOF) {
					break
				}
				// 帧头不足 20 字节：截断。
				if chainOpen {
					markBad(chainSegIdx, chainSegOff, CorruptionTruncated)
				} else {
					markBad(idx, frameStart, CorruptionTruncated)
				}
				break
			}
			if !h.headerOK() {
				if chainOpen {
					markBad(chainSegIdx, chainSegOff, CorruptionTruncated)
				} else {
					markBad(idx, frameStart, CorruptionMagic)
				}
				break
			}
			chunk, perr := fr.nextPayload(h.dataLen)
			if perr != nil {
				if chainOpen {
					markBad(chainSegIdx, chainSegOff, CorruptionTruncated)
				} else {
					markBad(idx, frameStart, CorruptionTruncated)
				}
				break
			}
			if crc32Chunk(chunk) != h.dataCRC {
				if chainOpen {
					markBad(chainSegIdx, chainSegOff, CorruptionTruncated)
				} else {
					markBad(idx, frameStart, CorruptionChecksum)
				}
				break
			}

			flags := h.flags & (flagFirst | flagLast)
			switch flags {
			case flagFirst: // 仅首帧
				if chainOpen {
					markBad(chainSegIdx, chainSegOff, CorruptionTruncated)
					break scanLoop
				}
				chainOpen = true
				chainSeq = h.seq
				chainSegIdx = idx
				chainSegOff = frameStart
			case flagFirst | flagLast: // 单帧完整记录
				if chainOpen {
					markBad(chainSegIdx, chainSegOff, CorruptionTruncated)
					break scanLoop
				}
				if haveRecords && h.seq != lastSeq+1 {
					markBad(idx, frameStart, CorruptionSequence)
					break scanLoop
				}
				if !haveRecords {
					sc.firstSeq = h.seq
				}
				sc.lastSeq = h.seq
				sc.haveRecords = true
				haveRecords = true
				lastSeq = h.seq
				sc.valid = hdrEnd + int64(h.dataLen)
			case flagLast: // 仅尾帧，闭合悬挂链
				if !chainOpen || h.seq != chainSeq {
					if chainOpen {
						markBad(chainSegIdx, chainSegOff, CorruptionTruncated)
					} else {
						markBad(idx, frameStart, CorruptionSequence)
					}
					break scanLoop
				}
				chainOpen = false
				if haveRecords && h.seq != lastSeq+1 {
					markBad(chainSegIdx, chainSegOff, CorruptionSequence)
					break scanLoop
				}
				// 闭合记录：把从链起点到本帧结尾的全部字节标记为有效。
				lastSeq = h.seq
				haveRecords = true
				scans[chainSegIdx].valid = chainSegOff // 链起点之前已有效
				markRecordValid(scans, chainSegIdx, chainSegOff, idx, hdrEnd+int64(h.dataLen), sc)
				sc.lastSeq = h.seq
				sc.haveRecords = true
				if !scans[chainSegIdx].haveRecords {
					scans[chainSegIdx].firstSeq = h.seq
				}
				scans[chainSegIdx].haveRecords = true
				scans[chainSegIdx].lastSeq = h.seq
			case 0: // 中间帧
				if !chainOpen || h.seq != chainSeq {
					if chainOpen {
						markBad(chainSegIdx, chainSegOff, CorruptionTruncated)
					} else {
						markBad(idx, frameStart, CorruptionSequence)
					}
					break scanLoop
				}
			}
			off = hdrEnd + int64(h.dataLen)
		}
		_ = fh.Close()
		if bad {
			break
		}
	}

	// 扫到流末仍有未闭合链：写入中途崩溃，整链丢弃。
	if !bad && chainOpen {
		markBad(chainSegIdx, chainSegOff, CorruptionTruncated)
	}

	// 计算每分段最终保留字节数。
	type keep struct {
		bytes int64
	}
	keeps := make([]keep, len(scans))
	if !bad {
		for i, sc := range scans {
			keeps[i] = keep{sc.size}
		}
	} else {
		for i, sc := range scans {
			switch {
			case i < badSegIdx:
				keeps[i] = keep{sc.size}
			case i == badSegIdx:
				keeps[i] = keep{badSegOff}
			default:
				keeps[i] = keep{0}
			}
		}
	}

	// 提交位置：最后一条完整闭合记录序号 +1；无记录则为首个分段 base。
	committed := files[0].base
	if haveRecords {
		committed = lastSeq + 1
	}

	// 落盘修复：截断损坏分段，删除其后分段，同步目录。
	var tailPath string
	var tailKept, tailDrop int64
	finalSegs := make([]*segment, 0, len(scans))
	var finalOffset int64
	for i, sc := range scans {
		k := keeps[i].bytes
		if k == 0 {
			if err := os.Remove(sc.seg.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
			continue
		}
		if k < sc.size {
			if err := truncateTail(sc.seg.path, k); err != nil {
				return nil, err
			}
			if tailPath == "" {
				tailPath = sc.seg.path
				tailKept = k
				tailDrop = sc.size - k
			}
			sc.seg.size = k
		}
		sc.seg.startOffset = finalOffset
		finalOffset += k
		finalSegs = append(finalSegs, sc.seg)
	}
	if tailPath != "" {
		if err := syncDir(dir); err != nil {
			return nil, err
		}
	}

	info.FirstPosition = files[0].base
	info.CommittedPosition = committed
	info.TailPath = tailPath
	info.KeptBytes = tailKept
	info.DroppedBytes = tailDrop
	if bad {
		info.Corruption = badKind
	}
	for _, s := range finalSegs {
		info.Segments = append(info.Segments, SegmentMeta{
			BasePosition: s.basePosition,
			Size:         s.size,
		})
	}
	switch info.Corruption {
	case CorruptionTruncated:
		info.Reason = "tail truncated: incomplete frame or unterminated multi-frame record discarded"
	case CorruptionChecksum:
		info.Reason = "checksum mismatch at tail; bytes from offending frame onward discarded"
	case CorruptionMagic:
		info.Reason = "unrecognizable frame magic/version at tail; remainder discarded"
	case CorruptionSequence:
		info.Reason = "non-contiguous record sequence at tail; remainder discarded"
	default:
		info.Reason = "clean: all frames form complete, verified, contiguous records"
	}
	return info, nil
}

// markRecordValid 把一条跨分段（或同分段）完整记录覆盖到的各分段
// valid 字段上：[startSeg:startOff, endSeg:endOff) 范围内所有字节均有效。
func markRecordValid(scans []*segScan, startSeg int, startOff int64, endSeg int, endOff int64, endScan *segScan) {
	for i := startSeg; i <= endSeg; i++ {
		switch {
		case i == startSeg && i == endSeg:
			scans[i].valid = endOff
		case i == startSeg:
			scans[i].valid = scans[i].size
		case i == endSeg:
			scans[i].valid = endOff
		default:
			scans[i].valid = scans[i].size
		}
	}
}

// truncateTail 将分段截断到 kept 字节并 fsync，使修复结论自身持久化。
func truncateTail(path string, kept int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(kept); err != nil {
		return err
	}
	return f.Sync()
}
