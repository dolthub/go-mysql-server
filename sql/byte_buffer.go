package sql

import (
	"sync"
)

const defaultByteBuffCap = 1024

var ByteBufPool = sync.Pool{
	New: func() any {
		return NewByteBuffer(defaultByteBuffCap)
	},
}

type ByteBuffer struct {
	buf []byte
}

func NewByteBuffer(initCap int) *ByteBuffer {
	return &ByteBuffer{buf: make([]byte, 0, initCap)}
}

func (b *ByteBuffer) Update(buf []byte) {
	if cap(buf) > cap(b.buf) {
		b.buf = buf
	}
}

func (b *ByteBuffer) Get() []byte {
	return b.buf[len(b.buf):]
}

func (b *ByteBuffer) Reset() {
	b.buf = b.buf[:0]
}
