package sql

import (
	"sync"
)

const defaultByteBuffCap = 4096

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

// Grow records the latest used byte position. Callers
// are responsible for accurately reporting which bytes
// they expect to be protected.
func (b *ByteBuffer) Grow(n int) {
	if b.i+n > len(b.buf) {
		// Runtime alloc'd into a separate backing array, but it chooses
		// the doubling cap using the non-optimal |cap(b.buf)-b.i|*2.
		// We do not need to increment |b.i| b/c the latest value is in
		// the other array.
		b.Double()
	} else {
		b.i += n
	}
}

// Double expands the backing array by 2x. We do this
// here because the runtime only doubles based on slice
// length.
func (b *ByteBuffer) Double() {
	buf := make([]byte, len(b.buf)*2)
	copy(buf, b.buf)
	b.buf = buf
}

// Get returns a zero length slice beginning at a safe
// write position.
func (b *ByteBuffer) Get() []byte {
	return b.buf[b.i:b.i]
}

func (b *ByteBuffer) Reset() {
	b.i = 0
}
