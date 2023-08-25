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
	"github.com/dolthub/go-mysql-server/sql/types"
)

type RowUpdateType int

const (
	UpdateTypeInsert RowUpdateType = iota
	UpdateTypeReplace
	UpdateTypeDuplicateKeyUpdate
	UpdateTypeUpdate
	UpdateTypeDelete
	UpdateTypeJoinUpdate
)

// RowUpdateAccumulator wraps other nodes that update tables, and returns their results as OKResults with the appropriate
// fields set.
type RowUpdateAccumulator struct {
	UnaryNode
	RowUpdateType
}

var _ sql.Node = RowUpdateAccumulator{}
var _ sql.CollationCoercible = RowUpdateAccumulator{}

// NewRowUpdateResult returns a new RowUpdateResult with the given node to wrap.
func NewRowUpdateAccumulator(n sql.Node, updateType RowUpdateType) *RowUpdateAccumulator {
	return &RowUpdateAccumulator{
		UnaryNode: UnaryNode{
			Child: n,
		},
		RowUpdateType: updateType,
	}
}

func (r RowUpdateAccumulator) Child() sql.Node {
	return r.UnaryNode.Child
}

func (r RowUpdateAccumulator) Schema() sql.Schema {
	return types.OkResultSchema
}

func (r RowUpdateAccumulator) IsReadOnly() bool {
	return r.Child().IsReadOnly()
}

func (r RowUpdateAccumulator) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, 1, len(children))
	}
	return NewRowUpdateAccumulator(children[0], r.RowUpdateType), nil
}

// CheckPrivileges implements the interface sql.Node.
func (r RowUpdateAccumulator) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return r.Child().CheckPrivileges(ctx, opChecker)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (r RowUpdateAccumulator) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, r.Child())
}

func (r RowUpdateAccumulator) String() string {
	return r.Child().String()
}

func (r RowUpdateAccumulator) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("RowUpdateAccumulator")
	_ = pr.WriteChildren(sql.DebugString(r.Child()))
	return pr.String()
}
