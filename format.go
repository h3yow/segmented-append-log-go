package seglog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// format.go：磁盘帧格式定义与编码/校验辅助。
//
// 帧（frame）是磁盘上的最小自描述单元；一条记录可由一个或多个帧组成：
//
//	+--------+----+----+--------+--------+----------+--------+
//	| magic  |ver |flg | seq    | datLen | dataCRC32| data   |
//	| 2 byte | 1B | 1B | 8 byte | 4 byte   4 byte   N byte |
//	+--------+----+----+--------+--------+----------+--------+
//
// magic   固定魔数，用于识别帧边界/文件归属；
// ver     帧格式版本（当前 FormatVersionV1=1）；
// flg     分片标志：flagFirst=0x1 记录首帧，flagLast=0x2 记录尾帧，
//         单帧记录同时置两者；
// seq     记录的逻辑序号（同一条记录的全部分片共享）；
// datLen  本分片负载字节数；
// dataCRC32 仅对本分片负载计算的 IEEE CRC-32。
//
// 恢复规则（与正常关闭无关，仅取决于磁盘字节）：从分段头顺序扫描，
// 帧头不完整、负载被截断、CRC 不符、magic 不符，或首/尾帧不配对的，
// 从该帧起始处起的全部字节都视为尾部残余并丢弃。
//
// 版本兼容规则：解析时只接受 ver == FormatVersionV1 的帧；遇到更高或
// 未知版本按 ErrInvalidMagic 同类的“不可识别帧”处理并停止扫描，
// 新版本组件读取旧版本目录时旧帧仍按其原有规则解释（前向兼容预留）。

const (
	frameMagic0 byte = 0x53 // 'S'
	frameMagic1 byte = 0x4c // 'L'

	// FormatVersionV1 是当前帧格式版本。
	FormatVersionV1 byte = 1

	flagFirst byte = 0x1
	flagLast  byte = 0x2

	frameHeaderSize = 2 + 1 + 1 + 8 + 4 + 4 // = 20
)

// frameHeader 为内存中使用的帧头视图。
type frameHeader struct {
	magic   [2]byte
	version byte
	flags   byte
	seq     uint64
	dataLen uint32
	dataCRC uint32
}

// encodeFrame 编码单个帧（含负载）。返回的切片可直接写入磁盘。
func encodeFrame(seq uint64, flags byte, chunk []byte) []byte {
	buf := make([]byte, frameHeaderSize+len(chunk))
	buf[0] = frameMagic0
	buf[1] = frameMagic1
	buf[2] = FormatVersionV1
	buf[3] = flags
	binary.BigEndian.PutUint64(buf[4:12], seq)
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(chunk)))
	binary.BigEndian.PutUint32(buf[16:20], crc32Chunk(chunk))
	copy(buf[frameHeaderSize:], chunk)
	return buf
}

// parseFrameHeader 解析帧头；长度不足时返回 false（属于截断）。
// 魔数/版本错误由调用方在解析后进一步区分归类。
func parseFrameHeader(b []byte) (frameHeader, bool) {
	if len(b) < frameHeaderSize {
		return frameHeader{}, false
	}
	var h frameHeader
	h.magic[0] = b[0]
	h.magic[1] = b[1]
	h.version = b[2]
	h.flags = b[3]
	h.seq = binary.BigEndian.Uint64(b[4:12])
	h.dataLen = binary.BigEndian.Uint32(b[12:16])
	h.dataCRC = binary.BigEndian.Uint32(b[16:20])
	return h, true
}

// headerOK 报告魔数与版本是否可识别。
func (h frameHeader) headerOK() bool {
	return h.magic[0] == frameMagic0 && h.magic[1] == frameMagic1 &&
		h.version == FormatVersionV1
}

// crcTable 为 IEEE CRC-32 表，进程内复用。
var crcTable = crc32.MakeTable(crc32.IEEE)

// crc32Chunk 计算单个分片负载的 CRC-32。
func crc32Chunk(chunk []byte) uint32 {
	return crc32.Checksum(chunk, crcTable)
}

// frameReader 在一个文件上顺序读取帧头与负载，复用读缓冲，避免
// 恢复扫描期间产生无界内存分配。
type frameReader struct {
	f   *os.File
	off int64
	buf [frameHeaderSize]byte
}

func newFrameReader(f *os.File) *frameReader {
	return &frameReader{f: f}
}

// nextHeader 读取下一帧头；干净到达 EOF 返回 io.EOF；帧头不完整返回
// io.ErrUnexpectedEOF（由调用方归类为截断）。返回的第二个值为帧头结束
// 后的全局偏移（即负载起始偏移）。
func (r *frameReader) nextHeader() (frameHeader, int64, error) {
	n, err := io.ReadFull(r.f, r.buf[:])
	if err != nil {
		if errors.Is(err, io.EOF) {
			return frameHeader{}, r.off, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			r.off += int64(n)
			return frameHeader{}, r.off, io.ErrUnexpectedEOF
		}
		return frameHeader{}, r.off, err
	}
	h, _ := parseFrameHeader(r.buf[:])
	off := r.off + frameHeaderSize
	r.off = off
	return h, off, nil
}

// nextPayload 读取 dataLen 字节负载；文件不足则返回 io.ErrUnexpectedEOF。
func (r *frameReader) nextPayload(dataLen uint32) ([]byte, error) {
	buf := make([]byte, dataLen)
	n, err := io.ReadFull(r.f, buf)
	r.off += int64(n)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return buf[:n], io.ErrUnexpectedEOF
		}
		return buf[:n], err
	}
	return buf, nil
}

// readFrameBounded 随机读取一帧并保证整个帧（帧头+负载）
// 都落在 [off, limit) 已提交区间内；越界视为不完整，绝不 pread 到
// 写入方尚未提交的字节。
func readFrameBounded(seg *segment, cache *fileCache, off, limit int64) (frameHeader, []byte, error) {
	if off+frameHeaderSize > limit {
		return frameHeader{}, nil, errFrameTruncated
	}
	if err := cache.pin(seg); err != nil {
		return frameHeader{}, nil, err
	}
	defer cache.unpin(seg)

	// 读锁与写路径的分段写锁配对，建立 happens-before；密封分段无竞争。
	seg.rwMu.RLock()
	defer seg.rwMu.RUnlock()

	var hdr [frameHeaderSize]byte
	if _, err := seg.readFile.ReadAt(hdr[:], off); err != nil {
		return frameHeader{}, nil, errFrameTruncated
	}
	h, _ := parseFrameHeader(hdr[:])
	if !h.headerOK() {
		return h, nil, nil
	}
	frameEnd := off + frameHeaderSize + int64(h.dataLen)
	if frameEnd > limit || frameEnd > seg.size {
		return h, nil, errFrameTruncated
	}
	payload := make([]byte, h.dataLen)
	if _, err := seg.readFile.ReadAt(payload, off+frameHeaderSize); err != nil {
		return h, nil, errFrameTruncated
	}
	return h, payload, nil
}

// errFrameTruncated 表示随机读取时帧头/负载超出分段可读/实际长度。
var errFrameTruncated = errors.New("seglog: frame truncated within segment")
