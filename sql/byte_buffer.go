package sql

import (
	"sync"
)

const defaultByteBuffCap = 10

var ByteBufPool = sync.Pool{
	New: func() any {
		return NewByteBuffer(defaultByteBuffCap)
	},
}

type ByteBuffer struct {
	i   int
	buf []byte
}

func NewByteBuffer(initCap int) *ByteBuffer {
	buf := make([]byte, initCap)
	return &ByteBuffer{buf: buf}
}

func (b *ByteBuffer) Update(buf []byte) {
	if b.i+len(buf) > len(b.buf) {
		// Runtime alloc'd into a separate backing array, but it chooses
		// the doubling cap using the non-optimal |cap(b.buf)-b.i|*2.
		// We do not need to increment |b.i| b/c the latest value is in
		// the other array.
		b.Double()
	} else {
		b.i += len(buf)
	}
}

func (b *ByteBuffer) Double() {
	buf := make([]byte, len(b.buf)*2)
	copy(buf, b.buf)
	b.buf = buf
}

func (b *ByteBuffer) Get() []byte {
	return b.buf[b.i:b.i]
}

func (b *ByteBuffer) Reset() {
	b.i = 0
}
