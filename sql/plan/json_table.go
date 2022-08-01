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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/oliveagle/jsonpath"
	"io"
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
	// TODO: just have one partition for now
	return &jsonTablePartitionIter{
		keys: [][]byte{{0}},
	}, nil
}

// PartitionRows implements the sql.Table interface
func (t *JSONTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return nil, nil
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
	// TODO: this
	return true
}

// TODO: do I need this? just make JSONTable have everything
type JSONTableNode struct {
}

// TODO: maybe just use in-memory table?
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
		data:   nil,
	}

	// TODO: use this data to somehow create a table
	// Do I create something in memory and have a RowIter?
	// Fill in "table" with data
	for _, col := range spec.Columns {
		colData, err := jsonpath.JsonPathLookup(jsonPathData, col.Type.Path)
		if err != nil {
			return nil, err
		}
		colDataArr, ok := colData.([]interface{})
		if !ok {
			panic("TODO: good error message")
		}
		for i, v := range colDataArr {

		}
		// TODO: worry about types later
		//table.data[col.Name.String()] = res
	}

	return table, nil
}
