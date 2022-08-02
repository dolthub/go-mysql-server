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

package plan

import (
	"encoding/json"
	"io"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/oliveagle/jsonpath"

	"github.com/dolthub/go-mysql-server/sql"
)

type jsonTablePartition struct {
	key []byte
}

var _ sql.Partition = &jsonTablePartition{}

func (j *jsonTablePartition) Key() []byte {
	return j.key
}

type jsonTablePartitionIter struct {
	keys [][]byte
	pos  int
}

var _ sql.PartitionIter = &jsonTablePartitionIter{}

func (j *jsonTablePartitionIter) Close(ctx *sql.Context) error {
	return nil
}

func (j *jsonTablePartitionIter) Next(ctx *sql.Context) (sql.Partition, error) {
	if j.pos >= len(j.keys) {
		return nil, io.EOF
	}

	key := j.keys[j.pos]
	j.pos++
	return &jsonTablePartition{key}, nil
}

type jsonTableRowIter struct {
	rows []sql.Row
	pos  int
}

var _ sql.RowIter = &jsonTableRowIter{}

func (j *jsonTableRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if j.pos >= len(j.rows) {
		return nil, io.EOF
	}
	row := j.rows[j.pos]
	j.pos++
	return row, nil
}

func (j *jsonTableRowIter) Close(ctx *sql.Context) error {
	return nil
}

type JSONTable struct {
	name   string
	schema sql.PrimaryKeySchema
	data   []sql.Row
	rowIdx uint64
}

var _ sql.Table = &JSONTable{}
var _ sql.Node = &JSONTable{}

// Name implements the sql.Table interface
func (t *JSONTable) Name() string {
	return t.name
}

// String implements the sql.Table interface
func (t *JSONTable) String() string {
	return t.name
}

// Schema implements the sql.Table interface
func (t *JSONTable) Schema() sql.Schema {
	return t.schema.Schema
}

// Partitions implements the sql.Table interface
func (t *JSONTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	// TODO: this does nothing
	return &jsonTablePartitionIter{
		keys: [][]byte{{0}},
	}, nil
}

// PartitionRows implements the sql.Table interface
func (t *JSONTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return &jsonTableRowIter{
		rows: t.data,
	}, nil
}

func (t *JSONTable) Resolved() bool {
	return true
}

func (t *JSONTable) Children() []sql.Node {
	return nil
}

func (t *JSONTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, nil
}

func (t *JSONTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	return t, nil
}

func (t *JSONTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// NewJSONTable creates a new in memory table from the JSON formatted data, a jsonpath path string, and table spec.
func NewJSONTable(data []byte, path string, spec *sqlparser.TableSpec, alias sqlparser.TableIdent, schema sql.PrimaryKeySchema) (sql.Node, error) {
	// Parse data as JSON
	var jsonData interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, err
	}

	// Get data specified from initial path
	jsonPathData, err := jsonpath.JsonPathLookup(jsonData, path)
	if err != nil {
		return nil, err
	}

	// Create new JSONTable node
	table := &JSONTable{
		name:   alias.String(),
		schema: schema,
	}

	// Allocate number of rows, based off values in first column
	if v, err := jsonpath.JsonPathLookup(jsonPathData, spec.Columns[0].Type.Path); err == nil {
		if val, ok := v.([]interface{}); ok {
			table.data = make([]sql.Row, len(val))
		}
	}

	// Fill in table with data
	for _, col := range spec.Columns {
		v, err := jsonpath.JsonPathLookup(jsonPathData, col.Type.Path)
		if err != nil {
			return nil, err
		}
		colData, ok := v.([]interface{})
		if !ok {
			panic("TODO: good error message")
		}
		for i, val := range colData {
			table.data[i] = append(table.data[i], val)
		}
	}

	return table, nil
}
