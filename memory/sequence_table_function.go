// Copyright 2023 Dolthub, Inc.
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

package memory

import (
	"encoding/binary"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	sqltypes "github.com/dolthub/go-mysql-server/sql/types"
)

var _ sql.TableFunction = (*SequenceTableFunction)(nil)
var _ sql.ExecSourceRel = (*SequenceTableFunction)(nil)
var _ sql.IndexAddressable = (*SequenceTableFunction)(nil)
var _ sql.IndexedTable = (*SequenceTableFunction)(nil)
var _ plan.TableNode = (*SequenceTableFunction)(nil)

type SequenceTableFunction struct {
	ctx *sql.Context

	min, max int
	exprs    []sql.Expression
	database sql.Database
}

var _ sql.Partition = (*sequencePartition)(nil)

type sequencePartition struct {
	min, max int
}

func (s *sequencePartition) Key() []byte {

	return binary.LittleEndian.AppendUint32(binary.LittleEndian.AppendUint32(nil, uint32(s.min)), uint32(s.max))
}

func (p *SequenceTableFunction) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// UnderlyingTable implements the plan.TableNode interface
func (p *SequenceTableFunction) UnderlyingTable() sql.Table {
	return p
}

// Collation implements the sql.Table interface.
func (p *SequenceTableFunction) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data. This data has a single partition.
func (p *SequenceTableFunction) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {

	return sql.PartitionsToPartitionIter(&sequencePartition{min: p.min, max: p.max}), nil
}

// PartitionRows is a sql.Table interface function that takes a partition and returns all rows in that partition.
// This table has a partition for just schema changes, one for just data changes, and one for both.
func (p *SequenceTableFunction) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return &SequenceTableFnRowIter{i: p.min, n: p.max}, nil
}

// LookupPartitions is a sql.IndexedTable interface function that takes an index lookup and returns the set of corresponding partitions.
func (p *SequenceTableFunction) LookupPartitions(context *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	min := lookup.Ranges[0][0].LowerBound.(sql.Below).Key.(int)
	max := lookup.Ranges[0][0].UpperBound.(sql.Above).Key.(int)
	return sql.PartitionsToPartitionIter(&sequencePartition{min: min, max: max}), nil
}

func (p *SequenceTableFunction) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	return p
}

func (p *SequenceTableFunction) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return []sql.Index{
		&Index{
			DB:           "",
			DriverName:   "",
			Tbl:          nil,
			TableName:    p.Name(),
			Exprs:        nil,
			Name:         "i",
			Unique:       false,
			Spatial:      false,
			Fulltext:     false,
			CommentStr:   "",
			PrefixLens:   nil,
			fulltextInfo: fulltextInfo{},
		},
	}, nil
}

var sequenceTableFunctionSchema = sql.Schema{
	&sql.Column{Name: "i", Type: sqltypes.Uint64, PrimaryKey: true, Nullable: false},
}

// NewInstance creates a new instance of TableFunction interface
func (p *SequenceTableFunction) NewInstance(ctx *sql.Context, db sql.Database, exprs []sql.Expression) (sql.Node, error) {
	newInstance := &SequenceTableFunction{
		ctx:      ctx,
		database: db,
	}

	node, err := newInstance.WithExpressions(exprs...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// Resolved implements the sql.Resolvable interface
func (p *SequenceTableFunction) Resolved() bool {
	return true
}

func (p *SequenceTableFunction) commitsResolved() bool {
	return true
}

// String implements the Stringer interface
func (p *SequenceTableFunction) String() string {
	return "sequence_table_function"
}

// Schema implements the sql.Node interface.
func (p *SequenceTableFunction) Schema() sql.Schema {
	return sequenceTableFunctionSchema
}

// Children implements the sql.Node interface.
func (p *SequenceTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface.
func (p *SequenceTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return p, nil
}

// CheckPrivileges implements the interface sql.Node.
func (p *SequenceTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// Expressions implements the sql.Expressioner interface.
func (p *SequenceTableFunction) Expressions() []sql.Expression {
	return p.exprs
}

// WithExpressions implements the sql.Expressioner interface.
func (p *SequenceTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	np := *p
	np.exprs = expression
	return &np, nil
}

// Database implements the sql.Databaser interface
func (p *SequenceTableFunction) Database() sql.Database {
	return p.database
}

// WithDatabase implements the sql.Databaser interface
func (p *SequenceTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	np := *p
	np.database = database
	return &np, nil
}

// Name implements the sql.TableFunction interface
func (p *SequenceTableFunction) Name() string {
	return p.String()
}

// RowIter implements the sql.ExecSourceRel interface
func (p *SequenceTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	partitions, err := p.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	return sql.NewTableRowIter(ctx, p, partitions), nil
}
