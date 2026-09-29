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

import "github.com/dolthub/go-mysql-server/sql"

// OnDuplicateKeyUpdateSource evaluates the duplicate-key assignments against a
// single existing/proposed row pair supplied by InsertInto. Its output is the
// OLD/NEW pair consumed by Update and its surrounding TriggerExecutor nodes.
// Child identifies the destination; it is not scanned for rows.
type OnDuplicateKeyUpdateSource struct {
	UnaryNode
	UpdateExprs *UpdateExprs
	Ignore      bool
}

func NewOnDuplicateKeyUpdateSource(destination sql.Node, exprs *UpdateExprs, ignore bool) *OnDuplicateKeyUpdateSource {
	return &OnDuplicateKeyUpdateSource{UnaryNode: UnaryNode{Child: destination}, UpdateExprs: exprs, Ignore: ignore}
}

func (n *OnDuplicateKeyUpdateSource) Schema(ctx *sql.Context) sql.Schema {
	return append(n.Child.Schema(ctx).Copy(), n.Child.Schema(ctx)...)
}

func (n *OnDuplicateKeyUpdateSource) Resolved() bool {
	return n.Child.Resolved() && n.UpdateExprs.Resolved()
}
func (n *OnDuplicateKeyUpdateSource) IsReadOnly() bool { return true }
func (n *OnDuplicateKeyUpdateSource) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("OnDuplicateKeyUpdateSource")
	_ = p.WriteChildren(n.Child.String())
	return p.String()
}
func (n *OnDuplicateKeyUpdateSource) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}
	nn := *n
	nn.Child = children[0]
	return &nn, nil
}
func (n *OnDuplicateKeyUpdateSource) Expressions() []sql.Expression {
	return n.UpdateExprs.AllExpressions()
}
func (n *OnDuplicateKeyUpdateSource) WithExpressions(ctx *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	exprs, err := n.UpdateExprs.WithExpressions(expressions)
	if err != nil {
		return nil, err
	}
	nn := *n
	nn.UpdateExprs = exprs
	return &nn, nil
}
