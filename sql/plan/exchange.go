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
	"fmt"

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

// ErrNoPartitionable is returned when no Partitionable node is found
// in the Exchange tree.
var ErrNoPartitionable = errors.NewKind("no partitionable node found in exchange tree")

// Exchange is a node that can parallelize the underlying tree iterating
// partitions concurrently.
type Exchange struct {
	UnaryNode
	Parallelism int
}

var _ sql.Node = (*Exchange)(nil)
var _ sql.CollationCoercible = (*Exchange)(nil)

// NewExchange creates a new Exchange node.
func NewExchange(
	parallelism int,
	child sql.Node,
) *Exchange {
	return &Exchange{
		UnaryNode:   UnaryNode{Child: child},
		Parallelism: parallelism,
	}
}

func (e *Exchange) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Exchange")
	_ = p.WriteChildren(e.Child.String())
	return p.String()
}

func (e *Exchange) DebugString() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Exchange(parallelism=%d)", e.Parallelism)
	_ = p.WriteChildren(sql.DebugString(e.Child))
	return p.String()
}

// WithChildren implements the Node interface.
func (e *Exchange) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}

	return NewExchange(e.Parallelism, children[0]), nil
}

// CheckPrivileges implements the interface sql.Node.
func (e *Exchange) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return e.Child.CheckPrivileges(ctx, opChecker)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (e *Exchange) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, e.Child)
}

type ExchangePartition struct {
	sql.Partition
	Table sql.Table
}

var _ sql.Node = (*ExchangePartition)(nil)

func (p *ExchangePartition) String() string {
	return fmt.Sprintf("Partition(%s)", string(p.Key()))
}

func (ExchangePartition) Children() []sql.Node { return nil }

func (ExchangePartition) Resolved() bool { return true }

func (p *ExchangePartition) Schema() sql.Schema {
	return p.Table.Schema()
}

// WithChildren implements the Node interface.
func (p *ExchangePartition) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}

	return p, nil
}

// CheckPrivileges implements the interface sql.Node.
func (p *ExchangePartition) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if node, ok := p.Table.(sql.Node); ok {
		return node.CheckPrivileges(ctx, opChecker)
	}
	// If the table is not a TableNode or other such node, then I guess we'll return true as to not fail.
	// This may not be the correct behavior though, as it's just a guess.
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (p *ExchangePartition) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	// This is inspired by CheckPrivileges, although it may not be the desired behavior in all circumstances
	if node, ok := p.Table.(sql.Node); ok {
		return sql.GetCoercibility(ctx, node)
	}
	return sql.Collation_binary, 7
}
