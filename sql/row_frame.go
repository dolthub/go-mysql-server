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
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
)

const (
	valueBackingArraySize = 64
	fieldBackingArraySize = 2048
)

// Row2 is a tuple of values.
type Row2 []Value

type Value struct { // 28 bytes
	Typ querypb.Type
	Val []byte
}

type RowFrame struct { // 3866 bytes

	// Values are the values this row.
	// If possible |vba| is used as the backing array
	// for Values. Additionaly, |fba| is used as the
	// backing array for each Value, if possible.
	Values []Value

	vba [valueBackingArraySize]Value
	fba [fieldBackingArraySize]byte
	off uint16
}
