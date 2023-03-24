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
	"fmt"
	"io"

	"github.com/oliveagle/jsonpath"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
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
	colPaths []string
	schema   sql.Schema
	data     []interface{}
	pos      int
}

var _ sql.RowIter = &jsonTableRowIter{}

func (j *jsonTableRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if j.pos >= len(j.data) {
		return nil, io.EOF
	}
	obj := j.data[j.pos]
	j.pos++

	row := make(sql.Row, len(j.colPaths))
	for i, p := range j.colPaths {
		var val interface{}
		if v, err := jsonpath.JsonPathLookup(obj, p); err == nil {
			val = v
		}

		value, err := j.schema[i].Type.Convert(val)
		if err != nil {
			return nil, err
		}

		row[i] = value
	}

	return row, nil
}

func (j *jsonTableRowIter) Close(ctx *sql.Context) error {
	return nil
}

type JSONTable struct {
	dataExpr sql.Expression
	name     string
	path     string
	schema   sql.PrimaryKeySchema
	colPaths []string
}

var _ sql.Table = &JSONTable{}
var _ sql.Node = &JSONTable{}
var _ sql.Expressioner = &JSONTable{}
var _ sql.CollationCoercible = &JSONTable{}

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

// Collation implements the sql.Table interface
func (t *JSONTable) Collation() sql.CollationID {
	return sql.Collation_Default
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
	return t.RowIter(ctx, nil)
}

// Resolved implements the sql.Resolvable interface
func (t *JSONTable) Resolved() bool {
	return t.dataExpr.Resolved()
}

// Children implements the sql.Node interface
func (t *JSONTable) Children() []sql.Node {
	return nil
}

// RowIter implements the sql.Node interface
func (t *JSONTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// data must evaluate to JSON string
	data, err := t.dataExpr.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	strData, err := types.LongBlob.Convert(data)
	if err != nil {
		return nil, fmt.Errorf("invalid data type for JSON data in argument 1 to function json_table; a JSON string or JSON type is required")
	}

	if strData == nil {
		return &jsonTableRowIter{}, nil
	}

	var jsonData interface{}
	if err := json.Unmarshal(strData.([]byte), &jsonData); err != nil {
		return nil, err
	}

	// Get data specified from initial path
	var jsonPathData []interface{}
	if rootJSONData, err := jsonpath.JsonPathLookup(jsonData, t.path); err == nil {
		if data, ok := rootJSONData.([]interface{}); ok {
			jsonPathData = data
		} else {
			jsonPathData = []interface{}{rootJSONData}
		}
	} else {
		return nil, err
	}

	return &jsonTableRowIter{
		colPaths: t.colPaths,
		schema:   t.schema.Schema,
		data:     jsonPathData,
	}, nil
}

// WithChildren implements the sql.Node interface
func (t *JSONTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	return t, nil
}

// CheckPrivileges implements the sql.Node interface
func (t *JSONTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*JSONTable) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// Expressions implements the sql.Expressioner interface
func (t *JSONTable) Expressions() []sql.Expression {
	return []sql.Expression{t.dataExpr}
}

// WithExpressions implements the sql.Expressioner interface
func (t *JSONTable) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) != 1 {
		return nil, sql.ErrInvalidExpressionNumber.New(t, len(expression), 1)
	}
	nt := *t
	nt.dataExpr = expression[0]
	return &nt, nil
}

// NewJSONTable creates a new in memory table from the JSON formatted data, a jsonpath path string, and table spec.
func NewJSONTable(ctx *sql.Context, dataExpr sql.Expression, path string, colPaths []string, alias string, schema sql.PrimaryKeySchema) (sql.Node, error) {
	if _, ok := dataExpr.(*Subquery); ok {
		return nil, sql.ErrInvalidArgument.New("JSON_TABLE")
	}

	return &JSONTable{
		name:     alias,
		dataExpr: dataExpr,
		path:     path,
		schema:   schema,
		colPaths: colPaths,
	}, nil
}
