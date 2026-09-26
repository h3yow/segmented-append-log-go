package seglog

import (
	"encoding/binary"
	"hash/crc32"
)

// 记录二进制格式（v1）：
//   [magic uint32 LE][length uint32 LE][crc32 uint32 LE][payload ...]
// magic 用于区分版本与定位记录边界；crc32 覆盖 length 与 payload。

const (
	recordMagic   uint32 = 0x53514C31 // "SQL1"，格式版本 v1
	recordHdrSize        = 12
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
