// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"sync"
)

// TODO: find optimal size
const bufCap = 4096 // 4KB

// byteBuffer serves as a statically sized backing array used to the wire methods (types.SQL() and types.SQLValue())
type byteBuffer struct {
	pos uint16
	buf []byte
}

var bufferPool = sync.Pool{
	New: func() any {
		return &byteBuffer{
			buf: make([]byte, bufCap),
		}
	},
}

// hasCapacity indicates if this buffer has `cap` bytes worth of capacity left
func (b *byteBuffer) hasCapacity(cap int) bool {
	return int(b.pos)+cap < bufCap
}

// Grow records the latest used byte position.
// Callers are responsible for accurately reporting which bytes they expect to be protected.
func (b *byteBuffer) grow(n int) {
	b.pos += uint16(n)
}

// Get returns a zero-length slice beginning at a safe writing position.
func (b *byteBuffer) get() []byte {
	return b.buf[b.pos:b.pos]
}

func (b *byteBuffer) reset() {
	b.pos = 0
}

// ByteBufferManager is responsible for handling all byteBuffers retrieved from byteBufferPool.
type ByteBufferManager struct {
	bufs []*byteBuffer
	cur  *byteBuffer
}

// NewByteBufferManager returns a ByteBufferManager with one byteBuffer already allocated.
func NewByteBufferManager() *ByteBufferManager {
	cur := bufferPool.Get().(*byteBuffer)
	cur.reset()
	return &ByteBufferManager{
		cur:  cur,
		bufs: make([]*byteBuffer, 0),
	}
}

// Get returns a zero-length slice guaranteed to have capacity for `cap` bytes.
// This function will retrieve any necessary byteBuffers from bufferPool.
func (b *ByteBufferManager) Get(cap int) []byte {
	if !b.cur.hasCapacity(cap) {
		b.bufs = append(b.bufs, b.cur)
		b.cur = bufferPool.Get().(*byteBuffer)
		b.cur.reset()
	}
	return b.cur.get()
}

// Grow shifts the safe writing position of the current byteBuffer.
func (b *ByteBufferManager) Grow(n int) {
	b.cur.grow(n)
}

// PutAll releases all allocated byteBuffers back into bufferPool.
func (b *ByteBufferManager) PutAll() {
	for _, buf := range b.bufs {
		bufferPool.Put(buf)
	}
	bufferPool.Put(b.cur)
}
