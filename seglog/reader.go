package seglog

// Reader 已注册读取方句柄。
// 读取方通过 Commit 显式上报消费进度；回收（手动 Reclaim 与容量不足时的
// 自动回收）只会清掉所有仍在跟的读取方都已经越过的数据。
// 读取方不再使用时必须 Close 注销，否则其进度会一直钉住回收界。
type Reader struct {
	l      *Log
	name   string
	closed bool
}

// RegisterReader 注册一个读取方。新注册的读取方尚未提交任何进度，
// 在首次 Commit 之前会钉住全部数据不被回收。
func (l *Log) RegisterReader(name string) (*Reader, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, ErrClosed
	}
	r := &Reader{l: l, name: name}
	l.readers[r] = 0
	return r, nil
}

// Name 返回注册时给定的读取方名称。
func (r *Reader) Name() string { return r.name }

// Commit 上报消费进度：off 为下一条待读的逻辑偏移，
// 即所有 < off 的记录都已被该读取方消费，可以被回收。
func (r *Reader) Commit(off uint64) error {
	r.l.mu.Lock()
	defer r.l.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	if _, ok := r.l.readers[r]; !ok {
		return ErrClosed
	}
	r.l.readers[r] = off
	return nil
}

// Close 注销读取方，之后不再参与回收界计算。
func (r *Reader) Close() error {
	r.l.mu.Lock()
	defer r.l.mu.Unlock()
	if !r.closed {
		r.closed = true
		delete(r.l.readers, r)
	}
	return nil
}
