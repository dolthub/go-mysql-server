// Copyright 2021 Dolthub, Inc.
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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
)

type IndexLookup struct {
	Expr   sql.Expression
	idx    ExpressionsIndex
	ranges sql.RangeCollection
}

var _ sql.IndexLookup = (*IndexLookup)(nil)

func NewIndexLookup(ctx *sql.Context, idx ExpressionsIndex, expr sql.Expression, ranges ...sql.Range) *IndexLookup {
	return &IndexLookup{
		Expr:   expr,
		idx:    idx,
		ranges: ranges,
	}
}

func (eil *IndexLookup) String() string {
	return eil.idx.ID()
}

func (eil *IndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	return &indexValIter{
		tbl:             eil.idx.MemTable(),
		partition:       p,
		matchExpression: eil.EvalExpression(),
	}, nil
}

func (eil *IndexLookup) Indexes() []string {
	return []string{eil.idx.ID()}
}

func (eil *IndexLookup) EvalExpression() sql.Expression {
	return eil.Expr
}

// Index implements the interface sql.IndexLookup.
func (eil *IndexLookup) Index() sql.Index {
	return eil.idx
}

// Ranges implements the interface sql.IndexLookup.
func (eil *IndexLookup) Ranges() sql.RangeCollection {
	return eil.ranges
}

// indexValIter does a very simple and verifiable iteration over the table values for a given index. It does this
// by iterating over all the table rows for a Partition and evaluating each of them for inclusion in the index. This is
// not an efficient way to store an index, and is only suitable for testing the correctness of index code in the engine.
type indexValIter struct {
	tbl             *Table
	partition       sql.Partition
	matchExpression sql.Expression
	values          [][]byte
	i               int
}

func (u *indexValIter) Next(*sql.Context) ([]byte, error) {
	err := u.initValues()
	if err != nil {
		return nil, err
	}

	if u.i < len(u.values) {
		valBytes := u.values[u.i]
		u.i++
		return valBytes, nil
	}

	return nil, io.EOF
}

func (u *indexValIter) initValues() error {
	if u.values == nil {
		rows, ok := u.tbl.partitions[string(u.partition.Key())]
		if !ok {
			return sql.ErrPartitionNotFound.New(u.partition.Key())
		}

		for i, row := range rows {
			res, err := sql.EvaluateCondition(sql.NewEmptyContext(), u.matchExpression, row)
			if err != nil {
				return err
			}

			if sql.IsTrue(res) {
				encoded, err := EncodeIndexValue(&IndexValue{
					Pos: i,
				})

				if err != nil {
					return err
				}

				u.values = append(u.values, encoded)
			}
		}
	}

	return nil
}

func (u *indexValIter) Close(_ *sql.Context) error {
	return nil
}
