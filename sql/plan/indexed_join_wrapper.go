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
	"github.com/dolthub/go-mysql-server/sql"
)

type IndexedJoinSorter struct {
	UnaryNode
	OldJoinSchema sql.Schema
}

var _ sql.Node = (*IndexedJoinSorter)(nil)

func NewIndexedJoinSorter(indexJoin sql.Node, oldJoinSchema sql.Schema) sql.Node {
	return &IndexedJoinSorter{
		UnaryNode:     UnaryNode{Child: indexJoin},
		OldJoinSchema: oldJoinSchema,
	}
}

func (i *IndexedJoinSorter) String() string {
	tp := sql.NewTreePrinter()
	_ = tp.WriteNode("IndexedJoinSorter()")
	_ = tp.WriteChildren(i.Child.String())
	return tp.String()
}

func (i *IndexedJoinSorter) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	itr, err := i.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return &indexedJoinSorterIter{
		indexedJoinIter:   itr,
		indexedJoinSchema: i.Child.Schema(),
		oldSchema:         i.OldJoinSchema,
	}, nil
}

func (i *IndexedJoinSorter) Schema() sql.Schema {
	return i.OldJoinSchema
}

type indexedJoinSorterIter struct {
	indexedJoinIter   sql.RowIter
	indexedJoinSchema sql.Schema
	oldSchema         sql.Schema
}

var _ sql.RowIter = (*indexedJoinSorterIter)(nil)

func (i *indexedJoinSorterIter) Next() (sql.Row, error) {
	next, err := i.indexedJoinIter.Next()
	if err != nil {
		return nil, err
	}

	converted := convertFromSchemaToSchema(next, i.oldSchema, i.indexedJoinSchema)

	return converted, nil
}

func convertFromSchemaToSchema(row sql.Row, oldSchema sql.Schema, newSchema sql.Schema) sql.Row {
	// Add some error handling here
	ret := make(sql.Row, len(newSchema))
	for i, c := range newSchema {
		idx := oldSchema.IndexOf(c.Name, c.Source)
		ret[idx] = row[i]
	}

	return ret
}

func (i *indexedJoinSorterIter) Close(context *sql.Context) error {
	return i.indexedJoinIter.Close(context)
}

func (i *IndexedJoinSorter) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}

	return NewIndexedJoinSorter(children[0], i.OldJoinSchema), nil
}
