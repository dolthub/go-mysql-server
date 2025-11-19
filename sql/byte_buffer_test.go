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
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGrowByteBuffer(t *testing.T) {
	b := NewByteBuffer(10)

	// grow less than boundary
	src1 := []byte{1, 1, 1}
	obj1 := append(b.Get(), src1...)
	b.Grow(len(src1))

	require.Equal(t, 10, len(b.buf))
	require.Equal(t, 3, b.i)
	require.Equal(t, 10, cap(obj1))

	// grow to boundary
	src2 := []byte{0, 0, 0, 0, 0, 0, 0}
	obj2 := append(b.Get(), src2...)
	b.Grow(len(src2))

	require.Equal(t, 20, len(b.buf))
	require.Equal(t, 10, b.i)
	require.Equal(t, 7, cap(obj2))

	src3 := []byte{2, 2, 2, 2, 2}
	obj3 := append(b.Get(), src3...)
	b.Grow(len(src3))

	require.Equal(t, 20, len(b.buf))
	require.Equal(t, 15, b.i)
	require.Equal(t, 10, cap(obj3))

	// grow exceeds boundary

	src4 := []byte{3, 3, 3, 3, 3, 3, 3, 3}
	obj4 := append(b.Get(), src4...)
	b.Grow(len(src4))

	require.Equal(t, 40, len(b.buf))
	require.Equal(t, 15, b.i)
	require.Equal(t, 16, cap(obj4))

	// objects are all valid after doubling
	require.Equal(t, src1, obj1)
	require.Equal(t, src2, obj2)
	require.Equal(t, src3, obj3)
	require.Equal(t, src4, obj4)

	// reset
	b.Reset()
	require.Equal(t, 40, len(b.buf))
	require.Equal(t, 0, b.i)
}

func TestByteBufferDoubling(t *testing.T) {
	bb := NewByteBuffer(5)
	fmt.Printf("bb.buf: %v, cap: %d\n", bb.buf, cap(bb.buf))
	fmt.Printf("bb.i:   %v\n", bb.i)

	i0 := bb.Get()
	i0 = strconv.AppendInt(i0, 12345, 10)
	bb.Grow(len(i0))
	fmt.Printf("i0:     %v, cap: %d\n", i0, cap(i0))
	fmt.Printf("bb.buf: %v, cap: %d\n", bb.buf, cap(bb.buf))
	fmt.Printf("bb.i:   %v\n", bb.i)

	i5 := bb.Get()
	i5 = strconv.AppendInt(i5, 678901, 10)
	bb.Grow(len(i5))
	fmt.Printf("i0:     %v, cap: %d\n", i0, cap(i0))
	fmt.Printf("i5:     %v, cap: %d\n", i5, cap(i5))
	fmt.Printf("bb.buf: %v, cap: %d\n", bb.buf, cap(bb.buf))
	fmt.Printf("bb.i:   %v\n", bb.i)
}
