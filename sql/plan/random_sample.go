// Copyright 2026 Dolthub, Inc.
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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// RandomSample executes an unseeded ORDER BY RAND() LIMIT n query by
// sampling random ordinal positions directly from an underlying
// [sql.OrdinalAddressableIndex] rather than performing a full table
// scan and filesort.
type RandomSample struct {
	TableNode sql.TableNode
	Table     sql.IndexAddressableTable
	Index     sql.OrdinalAddressableIndex
	Limit     sql.Expression
}

var _ sql.Node = (*RandomSample)(nil)
var _ sql.Expressioner = (*RandomSample)(nil)
var _ sql.CollationCoercible = (*RandomSample)(nil)

// NewRandomSample creates a new [*RandomSample] node.
func NewRandomSample(
	tableNode sql.TableNode,
	table sql.IndexAddressableTable,
	idx sql.OrdinalAddressableIndex,
	limit sql.Expression,
) *RandomSample {
	return &RandomSample{
		TableNode: tableNode,
		Table:     table,
		Index:     idx,
		Limit:     limit,
	}
}

// Resolved implements [sql.Node].
func (r *RandomSample) Resolved() bool {
	return r.TableNode.Resolved() && r.Limit.Resolved()
}

// String implements [sql.Node].
func (r *RandomSample) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode(fmt.Sprintf(
		"RandomSample(%s, %s, %s)",
		r.TableNode.Name(),
		r.Index.ID(),
		r.Limit.String(),
	))
	return pr.String()
}

// DebugString implements [sql.DebugStringer].
func (r *RandomSample) DebugString(ctx *sql.Context) string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode(fmt.Sprintf(
		"RandomSample(%s, %s, %s)",
		r.TableNode.Name(),
		r.Index.ID(),
		sql.DebugString(ctx, r.Limit),
	))
	return pr.String()
}

// Schema implements [sql.Node].
func (r *RandomSample) Schema(ctx *sql.Context) sql.Schema {
	return r.TableNode.Schema(ctx)
}

// Children implements [sql.Node].
func (r *RandomSample) Children() []sql.Node {
	return []sql.Node{r.TableNode}
}

// WithChildren implements [sql.Node].
func (r *RandomSample) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	tn, ok := children[0].(sql.TableNode)
	if !ok {
		return nil, fmt.Errorf("expected child to be sql.TableNode, got %T", children[0])
	}
	return &RandomSample{
		TableNode: tn,
		Table:     r.Table,
		Index:     r.Index,
		Limit:     r.Limit,
	}, nil
}

// Expressions implements [sql.Expressioner].
func (r *RandomSample) Expressions() []sql.Expression {
	return []sql.Expression{r.Limit}
}

// WithExpressions implements [sql.Expressioner].
func (r *RandomSample) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(exprs), 1)
	}
	return &RandomSample{
		TableNode: r.TableNode,
		Table:     r.Table,
		Index:     r.Index,
		Limit:     exprs[0],
	}, nil
}

// CollationCoercibility implements [sql.CollationCoercible].
func (r *RandomSample) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// IsReadOnly implements [sql.ReadOnlyNode].
func (r *RandomSample) IsReadOnly() bool {
	return true
}
