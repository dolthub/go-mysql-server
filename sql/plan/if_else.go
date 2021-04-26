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

package plan

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// IfConditional represents IF statements only.
type IfConditional struct {
	Condition sql.Expression
	Body      sql.Node
}

var _ sql.Node = (*IfConditional)(nil)
var _ sql.DebugStringer = (*IfConditional)(nil)
var _ sql.Expressioner = (*IfConditional)(nil)

// NewIfConditional creates a new *IfConditional node.
func NewIfConditional(condition sql.Expression, body sql.Node) *IfConditional {
	return &IfConditional{
		Condition: condition,
		Body:      body,
	}
}

// Resolved implements the sql.Node interface.
func (ic *IfConditional) Resolved() bool {
	return ic.Condition.Resolved() && ic.Body.Resolved()
}

// String implements the sql.Node interface.
func (ic *IfConditional) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode(fmt.Sprintf("IF(%s)", ic.Condition.String()))
	_ = p.WriteChildren(ic.Body.String())
	return p.String()
}

// DebugString implements the sql.DebugStringer interface.
func (ic *IfConditional) DebugString() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode(fmt.Sprintf("IF(%s)", sql.DebugString(ic.Condition)))
	_ = p.WriteChildren(sql.DebugString(ic.Body))
	return p.String()
}

// Schema implements the sql.Node interface.
func (ic *IfConditional) Schema() sql.Schema {
	return ic.Body.Schema()
}

// Children implements the sql.Node interface.
func (ic *IfConditional) Children() []sql.Node {
	return []sql.Node{ic.Body}
}

// WithChildren implements the sql.Node interface.
func (ic *IfConditional) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(ic, len(children), 1)
	}

	nic := *ic
	nic.Body = children[0]
	return &nic, nil
}

// Expressions implements the sql.Expressioner interface.
func (ic *IfConditional) Expressions() []sql.Expression {
	return []sql.Expression{ic.Condition}
}

// WithExpressions implements the sql.Expressioner interface.
func (ic *IfConditional) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(ic, len(exprs), 1)
	}

	nic := *ic
	nic.Condition = exprs[0]
	return &nic, nil
}

// RowIter implements the sql.Node interface.
func (ic *IfConditional) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return ic.Body.RowIter(ctx, row)
}

// IfElseBlock represents IF/ELSE IF/ELSE statements.
type IfElseBlock struct {
	IfConditionals []*IfConditional
	Else           sql.Node
}

var _ sql.Node = (*IfElseBlock)(nil)
var _ sql.DebugStringer = (*IfElseBlock)(nil)

// NewIfElse creates a new *IfElseBlock node.
func NewIfElse(ifConditionals []*IfConditional, elseStatement sql.Node) *IfElseBlock {
	return &IfElseBlock{
		IfConditionals: ifConditionals,
		Else:           elseStatement,
	}
}

// Resolved implements the sql.Node interface.
func (ieb *IfElseBlock) Resolved() bool {
	for _, s := range ieb.IfConditionals {
		if !s.Resolved() {
			return false
		}
	}
	return ieb.Else.Resolved()
}

// String implements the sql.Node interface.
func (ieb *IfElseBlock) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("IF BLOCK")
	var children []string
	for _, s := range ieb.IfConditionals {
		children = append(children, s.String())
	}
	_ = p.WriteChildren(children...)

	ep := sql.NewTreePrinter()
	_ = ep.WriteNode("ELSE")
	_ = ep.WriteChildren(ieb.Else.String())
	_ = p.WriteChildren(ep.String())

	return p.String()
}

// DebugString implements the sql.DebugStringer interface.
func (ieb *IfElseBlock) DebugString() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("IF BLOCK")
	var children []string
	for _, s := range ieb.IfConditionals {
		children = append(children, sql.DebugString(s))
	}
	_ = p.WriteChildren(children...)

	ep := sql.NewTreePrinter()
	_ = ep.WriteNode("ELSE")
	_ = ep.WriteChildren(sql.DebugString(ieb.Else))
	_ = p.WriteChildren(ep.String())

	return p.String()
}

// Schema implements the sql.Node interface.
func (ieb *IfElseBlock) Schema() sql.Schema {
	return nil
}

// Children implements the sql.Node interface.
func (ieb *IfElseBlock) Children() []sql.Node {
	statements := make([]sql.Node, len(ieb.IfConditionals)+1)
	for i, ifConditional := range ieb.IfConditionals {
		statements[i] = ifConditional
	}
	statements[len(ieb.IfConditionals)] = ieb.Else
	return statements
}

// WithChildren implements the sql.Node interface.
func (ieb *IfElseBlock) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) < 2 {
		return nil, fmt.Errorf("%T: invalid children number, got %d, expected at least 2", ieb, len(children))
	}
	ifConditionals := make([]*IfConditional, len(children)-1)
	for i, child := range children[:len(children)-1] {
		ifConditional, ok := child.(*IfConditional)
		if !ok {
			return nil, fmt.Errorf("%T: expected if conditional child but got %T", ieb, child)
		}
		ifConditionals[i] = ifConditional
	}
	return NewIfElse(ifConditionals, children[len(children)-1]), nil
}

// RowIter implements the sql.Node interface.
func (ieb *IfElseBlock) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var branchIter sql.RowIter

	var err error
	for _, ifConditional := range ieb.IfConditionals {
		condition, err := ifConditional.Condition.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		var passedCondition bool
		if condition != nil {
			passedCondition, err = sql.ConvertToBool(condition)
			if err != nil {
				return nil, err
			}
		}
		if !passedCondition {
			continue
		}

		branchIter, err = ifConditional.RowIter(ctx, row)
		if err != nil {
			return nil, err
		}
		// If the branchIter is already a block iter, then we don't need to construct our own, as its contained
		// node and schema will be a better representation of the iterated rows.
		if blockRowIter, ok := branchIter.(BlockRowIter); ok {
			return blockRowIter, nil
		}
		return &ifElseIter{
			branchIter: branchIter,
			sch:        ifConditional.Body.Schema(),
			branchNode: ifConditional.Body,
		}, nil
	}

	// All conditions failed so we run the else
	branchIter, err = ieb.Else.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}
	// If the branchIter is already a block iter, then we don't need to construct our own, as its contained
	// node and schema will be a better representation of the iterated rows.
	if blockRowIter, ok := branchIter.(BlockRowIter); ok {
		return blockRowIter, nil
	}
	return &ifElseIter{
		branchIter: branchIter,
		sch:        ieb.Else.Schema(),
		branchNode: ieb.Else,
	}, nil
}

// ifElseIter is the row iterator for *IfElseBlock.
type ifElseIter struct {
	branchIter sql.RowIter
	sch        sql.Schema
	branchNode sql.Node
}

var _ BlockRowIter = (*ifElseIter)(nil)

// Next implements the sql.RowIter interface.
func (i *ifElseIter) Next() (sql.Row, error) {
	return i.branchIter.Next()
}

// Close implements the sql.RowIter interface.
func (i *ifElseIter) Close(ctx *sql.Context) error {
	return i.branchIter.Close(ctx)
}

// RepresentingNode implements the sql.BlockRowIter interface.
func (i *ifElseIter) RepresentingNode() sql.Node {
	return i.branchNode
}

// Schema implements the sql.BlockRowIter interface.
func (i *ifElseIter) Schema() sql.Schema {
	return i.sch
}
