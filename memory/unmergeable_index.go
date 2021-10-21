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

package memory

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// A very dumb index that iterates over the rows of a table, evaluates its matching expressions against each row, and
// stores those values to be later retrieved. Only here to test the functionality of indexed queries. This kind of index
// cannot be merged with any other index.
type UnmergeableIndex struct {
	MergeableIndex
}

var _ sql.Index = (*UnmergeableIndex)(nil)
var _ sql.AscendIndex = (*UnmergeableIndex)(nil)
var _ sql.DescendIndex = (*UnmergeableIndex)(nil)
var _ sql.NegateIndex = (*UnmergeableIndex)(nil)

func (u *UnmergeableIndex) Get(key ...interface{}) (sql.IndexLookup, error) {
	return &UnmergeableIndexLookup{
		key: key,
		idx: u,
	}, nil
}

// UnmergeableIndexLookup is the only IndexLookup in this package that doesn't implement Mergeable, and therefore
// can't be merged with other lookups.
type UnmergeableIndexLookup struct {
	key []interface{}
	idx *UnmergeableIndex
}

func (u *UnmergeableIndexLookup) IsMergeable(_ sql.IndexLookup) bool {
	return false
}

func (u *UnmergeableIndexLookup) Intersection(_ ...sql.IndexLookup) (sql.IndexLookup, error) {
	panic("not mergeable!")
}

func (u *UnmergeableIndexLookup) Union(_ ...sql.IndexLookup) (sql.IndexLookup, error) {
	panic("not mergeable!")
}

var _ sql.IndexLookup = (*UnmergeableIndexLookup)(nil)
var _ sql.MergeableIndexLookup = (*UnmergeableIndexLookup)(nil)

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

func (u *indexValIter) Next() ([]byte, error) {
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

func getType(val interface{}) (interface{}, sql.Type) {
	switch val := val.(type) {
	case int:
		return int64(val), sql.Int64
	case uint:
		return int64(val), sql.Int64
	case int8:
		return int64(val), sql.Int64
	case uint8:
		return int64(val), sql.Int64
	case int16:
		return int64(val), sql.Int64
	case uint16:
		return int64(val), sql.Int64
	case int32:
		return int64(val), sql.Int64
	case uint32:
		return int64(val), sql.Int64
	case int64:
		return int64(val), sql.Int64
	case uint64:
		return int64(val), sql.Int64
	case float32:
		return float64(val), sql.Float64
	case float64:
		return float64(val), sql.Float64
	case string:
		return val, sql.LongText
	case nil:
		return nil, sql.Null
	case time.Time:
		return val, sql.Datetime
	default:
		panic(fmt.Sprintf("Unsupported type for %v of type %T", val, val))
	}
}

func (u *indexValIter) Close(_ *sql.Context) error {
	return nil
}

func (u *UnmergeableIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	var exprs []sql.Expression
	for exprI, expr := range u.idx.Exprs {
		lit, typ := getType(u.key[exprI])
		if typ == sql.Null {
			exprs = append(exprs, expression.NewIsNull(expr))
		} else {
			exprs = append(exprs, expression.NewEquals(expr, expression.NewLiteral(lit, typ)))
		}
	}

	return &indexValIter{
		tbl:             u.idx.Tbl,
		partition:       p,
		matchExpression: and(exprs...),
	}, nil
}

func (u *UnmergeableIndexLookup) Indexes() []string {
	var idxes = make([]string, len(u.key))
	for i, e := range u.idx.Exprs {
		idxes[i] = fmt.Sprint(e)
	}
	return idxes
}

func (u *UnmergeableIndex) Has(partition sql.Partition, key ...interface{}) (bool, error) {
	panic("not implemented")
}

func (u *UnmergeableIndexLookup) String() string {
	var idxes = make([]string, len(u.key))
	for i, e := range u.key {
		idxes[i] = fmt.Sprintf("%s %v", u.idx.Expressions()[i], e)
	}
	return strings.Join(idxes, ", ")
}

func (u *UnmergeableIndex) Table() string {
	return u.TableName
}
