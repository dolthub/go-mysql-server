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

package server

import (
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
)

const (
	defaultResultBufferSize = 1024 * 16
	rowsBatch               = 512
)

type resultBuilder struct {
	sch    sql.Schema
	types2 []sql.Type2

	rows [][]sqltypes.Value
	cnt  int

	buf []byte
	pos int
}

func newResultBuilder(sch sql.Schema) (bld *resultBuilder) {
	rows := make([][]sqltypes.Value, rowsBatch)
	for i := range rows {
		rows[i] = make([]sqltypes.Value, len(sch))
	}

	types := make([]sql.Type, len(sch))
	for i := range types {
		types[i] = sch[i].Type
	}

	bld = &resultBuilder{
		sch:  sch,
		rows: rows,
		buf:  make([]byte, defaultResultBufferSize),
	}
	return
}

func newResultBuilder2(sch sql.Schema) *resultBuilder {
	rows := make([][]sqltypes.Value, rowsBatch)
	for i := range rows {
		rows[i] = make([]sqltypes.Value, len(sch))
	}

	types2 := make([]sql.Type2, len(sch))
	for i := range types2 {
		types2[i] = sch[i].Type.(sql.Type2)
	}

	return &resultBuilder{
		sch:    sch,
		types2: types2,
		rows:   rows,
		buf:    make([]byte, defaultResultBufferSize),
	}
}

func (rb *resultBuilder) isFull() bool {
	return rb.cnt >= rowsBatch
}

func (rb *resultBuilder) result() *sqltypes.Result {
	return &sqltypes.Result{
		Fields:       schemaToFields(rb.sch),
		Rows:         rb.rows[:rb.cnt],
		RowsAffected: uint64(rb.cnt),
	}
}

func (rb *resultBuilder) reset() {
	rb.cnt = 0
	rb.pos = 0
}

func (rb *resultBuilder) writeRow(row sql.Row) error {
	r := rb.rows[rb.cnt]

	var err error
	for i, v := range row {
		if v == nil {
			r[i] = sqltypes.NULL
			continue
		}
		r[i], err = rb.sch[i].Type.SQL(v)
		if err != nil {
			return err
		}
	}
	rb.cnt++

	return nil
}

func (rb *resultBuilder) writeRow2(row sql.Row2) (err error) {
	r := rb.rows[rb.cnt]

	for i := range row.Values {
		f := row.GetField(i)
		if f.IsNull() {
			r[i] = sqltypes.NULL
			continue
		}

		typ := rb.types2[i]
		dst := rb.buf[:rb.pos]

		r[i], err = typ.SQL2(dst, f)
		if err != nil {
			return err
		}

		rb.pos += r[i].Len()
	}
	rb.cnt++

	return
}
