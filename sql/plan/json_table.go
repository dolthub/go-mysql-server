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
	"io"

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


type JSONTableColOpts struct {
	Path            string
	Exists          bool
	DefaultErrorVal sql.Expression
	DefaultEmptyVal sql.Expression
	ErrorOnError    bool
	ErrorOnEmpty    bool
	ColOpts         []JSONTableColOpts
}

type JSONTable struct {
	DataExpr  sql.Expression
	TableName string
	Path      string
	Sch       sql.PrimaryKeySchema
	ColOpts   []JSONTableColOpts
	b         sql.NodeExecBuilder
}

var _ sql.Table = (*JSONTable)(nil)
var _ sql.Node = (*JSONTable)(nil)
var _ sql.Expressioner = (*JSONTable)(nil)
var _ sql.CollationCoercible = (*JSONTable)(nil)

// Name implements the sql.Table interface
func (t *JSONTable) Name() string {
	return t.TableName
}

// String implements the sql.Table interface
func (t *JSONTable) String() string {
	return t.TableName
}

// Schema implements the sql.Table interface
func (t *JSONTable) Schema() sql.Schema {
	return t.Sch.Schema
}

// Collation implements the sql.Table interface
func (t *JSONTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements the sql.Table interface
func (t *JSONTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	// TODO: this does Nothing
	return &jsonTablePartitionIter{
		keys: [][]byte{{0}},
	}, nil
}

// PartitionRows implements the sql.Table interface
func (t *JSONTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return t.b.Build(ctx, t, nil)
}

// Resolved implements the sql.Resolvable interface
func (t *JSONTable) Resolved() bool {
	if !t.DataExpr.Resolved() {
		return false
	}
	for _, opt := range t.ColOpts {
		if !opt.DefaultErrorVal.Resolved() || !opt.DefaultEmptyVal.Resolved() {
			return false
		}
	}
	return true
}

// Children implements the sql.Node interface
func (t *JSONTable) Children() []sql.Node {
	return nil
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
	return []sql.Expression{t.DataExpr} // TODO: better to do nothing or return all exprs?
}

// WithExpressions implements the sql.Expressioner interface
func (t *JSONTable) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	// TODO: better to do nothing or return all exprs?
	if len(expression) != 1 {
		return nil, sql.ErrInvalidExpressionNumber.New(t, len(expression), 1)
	}
	nt := *t
	nt.DataExpr = expression[0]
	return &nt, nil
}

// NewJSONTable creates a new in memory table from the JSON formatted data, a jsonpath path string, and table spec.
func NewJSONTable(dataExpr sql.Expression, path string, alias string, schema sql.PrimaryKeySchema, colOpts []JSONTableColOpts) (sql.Node, error) {
	if _, ok := dataExpr.(*Subquery); ok {
		return nil, sql.ErrInvalidArgument.New("JSON_TABLE")
	}

	return &JSONTable{
		TableName: alias,
		DataExpr:  dataExpr,
		Path:      path,
		Sch:       schema,

		ColOpts: colOpts,
	}, nil
}
