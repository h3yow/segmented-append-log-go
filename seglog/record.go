package seglog

import (
	"encoding/binary"
	"hash/crc32"
)

// 记录二进制格式（v1）：
//   [magic uint32 LE][length uint32 LE][crc32 uint32 LE][payload ...]
// magic 用于区分版本与定位记录边界；crc32 覆盖 length 与 payload。

const (
	recordMagic   uint32 = 0x53514C31 // "SQL1"，格式版本 v1（整记录，单段内）
	recordHdrSize        = 12
)

// 分片格式（v2）：仅当一条记录放不进任何一个空段时，把它拆成多个分片，
// 连续写入后续分段。每个分片自包含、可独立校验：
//
//	[magic uint32 LE][length uint32 LE][crc32 uint32 LE][flags uint8][chunk ...]
//
// length = 1 + len(chunk)；crc32 覆盖 length 字段、flags 与 chunk。
// 一条逻辑记录 = FIRST 分片 + 0..n 个中间分片 + LAST 分片，片段在段序列中连续。
const (
	fragMagic   uint32 = 0x53514C32 // "SQL2"，格式版本 v2（跨段记录的分片）
	fragHdrSize        = 13
)

const (
	fragFirst uint8 = 1 << 0 // 记录的首个分片
	fragLast  uint8 = 1 << 1 // 记录的最后一个分片
)

// encodeRecord 将 payload 编码为一条完整记录，返回编码后的字节。
func encodeRecord(payload []byte) []byte {
	buf := make([]byte, recordHdrSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], recordMagic)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(payload)))
	crc := crc32.NewIEEE()
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	crc.Write(lenBuf[:])
	crc.Write(payload)
	binary.LittleEndian.PutUint32(buf[8:12], crc.Sum32())
	copy(buf[recordHdrSize:], payload)
	return buf
}

// decodeRecord 从 buf 起始处解析一条记录。
// 返回 payload、消耗的字节数；失败时返回 *CorruptError 说明可区分原因。
// 缓冲区不足以容纳完整记录时返回 CorruptTornTail。
func decodeRecord(buf []byte) (payload []byte, consumed int, err error) {
	if len(buf) < recordHdrSize {
		return nil, 0, &CorruptError{Kind: CorruptTornTail, Detail: "incomplete record header"}
	}
	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != recordMagic {
		return nil, 0, &CorruptError{Kind: CorruptBadMagic, Detail: "magic mismatch"}
	}
	length := binary.LittleEndian.Uint32(buf[4:8])
	if length > 1<<31 {
		return nil, 0, &CorruptError{Kind: CorruptBadLength, Detail: "implausible record length"}
	}
	total := recordHdrSize + int(length)
	if len(buf) < total {
		return nil, 0, &CorruptError{Kind: CorruptTornTail, Detail: "incomplete record payload"}
	}
	wantCRC := binary.LittleEndian.Uint32(buf[8:12])
	crc := crc32.NewIEEE()
	crc.Write(buf[4:8])
	crc.Write(buf[recordHdrSize:total])
	if crc.Sum32() != wantCRC {
		return nil, 0, &CorruptError{Kind: CorruptChecksum, Detail: "crc32 mismatch"}
	}
	return buf[recordHdrSize:total], total, nil
}

// encodeFragment 把记录的一段字节编码为一个分片。
func encodeFragment(flags uint8, chunk []byte) []byte {
	buf := make([]byte, fragHdrSize+len(chunk))
	binary.LittleEndian.PutUint32(buf[0:4], fragMagic)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(1+len(chunk)))
	crc := crc32.NewIEEE()
	crc.Write(buf[4:8])
	buf[12] = flags
	crc.Write(buf[12:13])
	crc.Write(chunk)
	binary.LittleEndian.PutUint32(buf[8:12], crc.Sum32())
	copy(buf[fragHdrSize:], chunk)
	return buf
}

// decodeFragment 从 buf 起始处解析一个分片。
// 返回 flags、chunk 字节与消耗的字节数；失败时返回 *CorruptError，原因可区分。
// 缓冲区不足以容纳完整分片时返回 CorruptTornTail。
func decodeFragment(buf []byte) (flags uint8, chunk []byte, consumed int, err error) {
	if len(buf) < fragHdrSize {
		return 0, nil, 0, &CorruptError{Kind: CorruptTornTail, Detail: "incomplete fragment header"}
	}
	if magic := binary.LittleEndian.Uint32(buf[0:4]); magic != fragMagic {
		return 0, nil, 0, &CorruptError{Kind: CorruptBadMagic, Detail: "fragment magic mismatch"}
	}
	length := binary.LittleEndian.Uint32(buf[4:8])
	if length < 1 || length > 1<<31 {
		return 0, nil, 0, &CorruptError{Kind: CorruptBadLength, Detail: "implausible fragment length"}
	}
	total := fragHdrSize - 1 + int(length)
	if len(buf) < total {
		return 0, nil, 0, &CorruptError{Kind: CorruptTornTail, Detail: "incomplete fragment payload"}
	}
	wantCRC := binary.LittleEndian.Uint32(buf[8:12])
	crc := crc32.NewIEEE()
	crc.Write(buf[4:8])
	crc.Write(buf[12:total])
	if crc.Sum32() != wantCRC {
		return 0, nil, 0, &CorruptError{Kind: CorruptChecksum, Detail: "fragment crc32 mismatch"}
	}
	return buf[12], buf[fragHdrSize:total], total, nil
}
