package sql

var SingletonBuf = NewByteBuffer(16000)

type ByteBuffer struct {
	buf []byte
	i   int
}

func NewByteBuffer(initCap int) *ByteBuffer {
	return &ByteBuffer{buf: make([]byte, initCap)}
}

func (b *ByteBuffer) Bytes() []byte {
	return b.buf
}

func (b *ByteBuffer) GetFull(i int) []byte {
	start := b.i
	b.i = start + i
	if b.i > len(b.buf) {
		newBuf := make([]byte, len(b.buf)*2)
		copy(newBuf, b.buf[:])
		b.buf = newBuf
	}
	return b.buf[start:b.i]
}

func (b *ByteBuffer) Double() {
	newBuf := make([]byte, len(b.buf)*2)
	copy(newBuf, b.buf[:])
	b.buf = newBuf
}

func (b *ByteBuffer) Advance(i int) {
	b.i += i
}

func (b *ByteBuffer) Spare() int {
	return len(b.buf) - b.i
}

func (b *ByteBuffer) Get() []byte {
	//start := b.i
	//b.i = start + i
	//if b.i > len(b.buf) {
	//	newBuf := make([]byte, len(b.buf)*2)
	//	copy(newBuf, b.buf[:])
	//	b.buf = newBuf
	//}
	//return b.buf[start:b.i][:0]
	return b.buf[b.i:b.i]
}

func (b *ByteBuffer) Reset() {
	b.i = 0
}
