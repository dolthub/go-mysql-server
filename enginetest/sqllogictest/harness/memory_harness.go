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

package harness

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/shopspring/decimal"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

type memoryHarness struct {
	engine  *sqle.Engine
	harness enginetest.VersionedDBHarness
}

func NewMemoryHarness(harness enginetest.VersionedDBHarness) *memoryHarness {
	return &memoryHarness{
		harness: harness,
	}
}

func (h *memoryHarness) EngineStr() string {
	return "mysql"
}

func (h *memoryHarness) Init() error {
	dbs := h.harness.NewDatabases("mydb")
	pro := memory.NewDBProvider(dbs...)
	h.engine = sqle.NewDefault(pro)
	return nil
}

func (h *memoryHarness) ExecuteStatement(statement string) error {
	ctx := h.newContext()

	_, rowIter, _, err := h.engine.Query(ctx, statement)
	if err != nil {
		return err
	}

	return enginetest.DrainIterator(ctx, rowIter)
}

var pid uint32

func (h *memoryHarness) newContext() *sql.Context {
	ctx := h.harness.NewContext()
	ctx.SetCurrentDatabase("mydb")
	ctx.ApplyOpts(sql.WithPid(uint64(atomic.AddUint32(&pid, 1))))
	return ctx
}

func (h *memoryHarness) ExecuteQuery(statement string) (schema string, results []string, err error) {
	ctx := h.newContext()

	var sch sql.Schema
	var rowIter sql.RowIter
	defer func() {
		if r := recover(); r != nil {
			// Panics leave the engine in a bad state that we have to clean up
			h.engine.ProcessList.Kill(pid)
			panic(r)
		}
	}()

	sch, rowIter, _, err = h.engine.Query(ctx, statement)
	if err != nil {
		return "", nil, err
	}

	schemaString, err := schemaToSchemaString(sch)
	if err != nil {
		return "", nil, err
	}

	results, err = rowsToResultStrings(ctx, rowIter)
	if err != nil {
		return "", nil, err
	}

	return schemaString, results, nil
}

// Returns the rows in the iterator given as an array of their string representations, as expected by the test files
func rowsToResultStrings(ctx *sql.Context, iter sql.RowIter) ([]string, error) {
	var results []string
	if iter == nil {
		return results, nil
	}

	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			return results, nil
		} else if err != nil {
			enginetest.DrainIteratorIgnoreErrors(ctx, iter)
			return nil, err
		} else {
			for _, col := range row.Values() {
				results = append(results, toSqlString(col))
			}
		}
	}
}

func toSqlString(val interface{}) string {
	if val == nil {
		return "NULL"
	}

	switch v := val.(type) {
	case float32, float64:
		// exactly 3 decimal points for floats
		return fmt.Sprintf("%.3f", v)
	case decimal.Decimal:
		// exactly 3 decimal points for floats
		return v.StringFixed(3)
	case int:
		return strconv.Itoa(v)
	case uint:
		return strconv.Itoa(int(v))
	case int8:
		return strconv.Itoa(int(v))
	case uint8:
		return strconv.Itoa(int(v))
	case int16:
		return strconv.Itoa(int(v))
	case uint16:
		return strconv.Itoa(int(v))
	case int32:
		return strconv.Itoa(int(v))
	case uint32:
		return strconv.Itoa(int(v))
	case int64:
		return strconv.Itoa(int(v))
	case uint64:
		return strconv.Itoa(int(v))
	case string:
		return v
	// Mysql returns 1 and 0 for boolean values, mimic that
	case bool:
		if v {
			return "1"
		} else {
			return "0"
		}
	default:
		panic(fmt.Sprintf("No conversion for value %v of type %T", val, val))
	}
}

func schemaToSchemaString(sch sql.Schema) (string, error) {
	b := strings.Builder{}
	for _, col := range sch {
		switch col.Type.Type() {
		case query.Type_INT8, query.Type_INT16, query.Type_INT24, query.Type_INT32, query.Type_INT64,
			query.Type_UINT8, query.Type_UINT16, query.Type_UINT24, query.Type_UINT32, query.Type_UINT64,
			query.Type_BIT:
			b.WriteString("I")
		case query.Type_TEXT, query.Type_VARCHAR:
			b.WriteString("T")
		case query.Type_FLOAT32, query.Type_FLOAT64, query.Type_DECIMAL:
			b.WriteString("R")
		default:
			return "", fmt.Errorf("Unhandled type: %v", col.Type)
		}
	}
	return b.String(), nil
}
