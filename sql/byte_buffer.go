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
const buffCap = 4096 // 4KB

var ByteBufPool = sync.Pool{
	New: func() any {
		return NewByteBuffer()
	},
}

type ByteBuffer struct {
	pos uint16
	buf [buffCap]byte
}

func NewByteBuffer() *ByteBuffer {
	return &ByteBuffer{}
}

// HasCapacity indicates if this buffer has `n` bytes worth of capacity left
func (b *ByteBuffer) HasCapacity(n int) bool {
	return int(b.pos)+n < buffCap
}

// Grow records the latest used byte position. Callers
// are responsible for accurately reporting which bytes
// they expect to be protected.
func (b *ByteBuffer) Grow(n int) {
	b.pos += uint16(n)
}

// Get returns a zero-length slice beginning at a safe
// write position.
func (b *ByteBuffer) Get() []byte {
	return b.buf[b.pos:b.pos]
}

func (b *ByteBuffer) Reset() {
	b.pos = 0
}
