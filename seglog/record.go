package seglog

import (
	"encoding/binary"
	"hash/crc32"
)

// 记录二进制格式（v1）：
//   [magic uint32 LE][length uint32 LE][crc32 uint32 LE][payload ...]
// magic 用于区分版本与定位记录边界；crc32 覆盖 length 与 payload。
//
// 跨段记录（v2）：一条放不进当前段剩余空间的记录被拆成连续的多个块，
// 每块仍使用上述 12 字节头，仅 magic 不同：
//   magicChunkFirst  首块（记录在此分配逻辑偏移）
//   magicChunkMiddle 中间块
//   magicChunkLast   末块
// 单块即可容纳的记录仍使用 recordMagic（v1 数据完全兼容）。

const (
	recordMagic      uint32 = 0x53514C31 // "SQL1"，完整记录（v1）
	magicChunkFirst  uint32 = 0x53514C46 // "SQLF"，跨段记录首块
	magicChunkMiddle uint32 = 0x53514C4D // "SQLM"，跨段记录中间块
	magicChunkLast   uint32 = 0x53514C4C // "SQLL"，跨段记录末块
	recordHdrSize           = 12
)

// isChunkContinuation 报告 magic 是否为续块（中/尾块）。
func isChunkContinuation(magic uint32) bool {
	return magic == magicChunkMiddle || magic == magicChunkLast
}

// isKnownMagic 报告 magic 是否为已知的记录/块类型。
func isKnownMagic(magic uint32) bool {
	return magic == recordMagic || magic == magicChunkFirst ||
		magic == magicChunkMiddle || magic == magicChunkLast
}

// encodeRecord 将 payload 编码为一条完整记录，返回编码后的字节。
func encodeRecord(payload []byte) []byte {
	return encodeChunk(recordMagic, payload)
}

// encodeChunk 将 payload 编码为指定 magic 的一个块，返回编码后的字节。
func encodeChunk(magic uint32, payload []byte) []byte {
	buf := make([]byte, recordHdrSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], magic)
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

// decodeRecord 从 buf 起始处解析一条记录/一个块。
// 返回 magic、payload、消耗的字节数；失败时返回 *CorruptError 说明可区分原因。
// 缓冲区不足以容纳完整记录时返回 CorruptTornTail。
func decodeRecord(buf []byte) (magic uint32, payload []byte, consumed int, err error) {
	if len(buf) < recordHdrSize {
		return 0, nil, 0, &CorruptError{Kind: CorruptTornTail, Detail: "incomplete record header"}
	}
	magic = binary.LittleEndian.Uint32(buf[0:4])
	if !isKnownMagic(magic) {
		return 0, nil, 0, &CorruptError{Kind: CorruptBadMagic, Detail: "magic mismatch"}
	}
	length := binary.LittleEndian.Uint32(buf[4:8])
	if length > 1<<31 {
		return 0, nil, 0, &CorruptError{Kind: CorruptBadLength, Detail: "implausible record length"}
	}
	total := recordHdrSize + int(length)
	if len(buf) < total {
		return 0, nil, 0, &CorruptError{Kind: CorruptTornTail, Detail: "incomplete record payload"}
	}
	wantCRC := binary.LittleEndian.Uint32(buf[8:12])
	crc := crc32.NewIEEE()
	crc.Write(buf[4:8])
	crc.Write(buf[recordHdrSize:total])
	if crc.Sum32() != wantCRC {
		return 0, nil, 0, &CorruptError{Kind: CorruptChecksum, Detail: "crc32 mismatch"}
	}
	return magic, buf[recordHdrSize:total], total, nil
}
