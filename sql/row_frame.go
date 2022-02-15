// Copyright 2020-2021 Dolthub, Inc.
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

	querypb "github.com/dolthub/vitess/go/vt/proto/query"
)

const (
	valueArrSize = 64
	fieldArrSize = 2048
)

// Row2 is a tuple of values.
type Row2 []Value

type Value struct {
	Typ querypb.Type
	Val []byte
}

// IsNull returns whether this value represents NULL
func (v Value) IsNull() bool {
	return v.Val == nil || v.Typ == querypb.Type_NULL_TYPE
}

type RowFrame struct {

	// Values are the values this row.
	Values []Value

	// varr is used as the backing array for the |Values|
	// slice when len(Values) <= valueArrSize
	varr [valueArrSize]Value

	// farr is used as the backing array for |Value.Val|
	// slices when there is capacity
	farr [fieldArrSize]byte

	// off tracks the next available position in |farr|
	off uint16
}

func NewRowFrame(vals ...Value) (f *RowFrame) {
	f = framePool.Get().(*RowFrame)
	f.Append(vals...)
	return
}

var framePool = sync.Pool{New: makeRowFrame}

func makeRowFrame() interface{} {
	return &RowFrame{}
}

// Row2 returns the underlying row value in this frame. Does not make a deep copy of underlying byte arrays, so
// further modification to this frame may result in the returned value changing as well.
func (f *RowFrame) Row2() Row2 {
	return f.Values
}

// Row2Copy returns the row in this frame as a deep copy of the underlying byte arrays. Useful when reusing the
// rowframe object.
func (f *RowFrame) Row2Copy() Row2 {
	r := make(Row2, len(f.Values))
	for i := range f.Values {
		v := f.Values[i]
		v.Val = make([]byte, len(v.Val))
		copy(v.Val, f.Values[i].Val)
		r[i] = v
	}
	return r
}

// Clear clears this row frame for reuse. The underlying byte arrays are not zeroed out or discarded, but will be
// overwritten by future calls to Append.
func (f *RowFrame) Clear() {
	f.Values = f.Values[:0]
	f.off = 0
}

func (f *RowFrame) Append(vals ...Value) {
	for _, v := range vals {
		f.append(v)
	}
}

func (f *RowFrame) append(v Value) {
	buf := f.getBuffer(v)
	copy(buf, v.Val)
	v.Val = buf

	// if |f.Values| grows past |len(f.varr)|
	// we'll allocate a new backing array here
	f.Values = append(f.Values, v)
}

func (f *RowFrame) getBuffer(v Value) (buf []byte) {
	if f.checkCapacity(v) {
		start := f.off
		f.off += uint16(len(v.Val))
		stop := f.off
		buf = f.farr[start:stop]
	} else {
		buf = make([]byte, len(v.Val))
	}

	return
}

func (f *RowFrame) checkCapacity(v Value) bool {
	return len(v.Val) <= (len(f.farr) - int(f.off))
}
