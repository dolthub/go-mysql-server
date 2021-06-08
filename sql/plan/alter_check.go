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
	"io"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

var (
	// ErrNoCheckConstraintSupport is returned when the table does not support CONSTRAINT CHECK operations.
	ErrNoCheckConstraintSupport = errors.NewKind("the table does not support check constraint operations: %s")

	// ErrCheckFailed is returned when the check constraint evaluates to false
	ErrCheckFailed = errors.NewKind("check constraint %s is violated.")
)

type CreateCheck struct {
	UnaryNode
	Check *sql.CheckConstraint
}

type DropCheck struct {
	UnaryNode
	Name string
}

func NewAlterAddCheck(table sql.Node, check *sql.CheckConstraint) *CreateCheck {
	return &CreateCheck{
		UnaryNode: UnaryNode{table},
		Check:     check,
	}
}

func NewAlterDropCheck(table sql.Node, name string) *DropCheck {
	return &DropCheck{
		UnaryNode: UnaryNode{Child: table},
		Name:      name,
	}
}

func getCheckAlterable(node sql.Node) (sql.CheckAlterableTable, error) {
	switch node := node.(type) {
	case sql.CheckAlterableTable:
		return node, nil
	case *ResolvedTable:
		return getCheckAlterableTable(node.Table)
	default:
		return nil, ErrNoCheckConstraintSupport.New(node.String())
	}
}

func getCheckAlterableTable(t sql.Table) (sql.CheckAlterableTable, error) {
	switch t := t.(type) {
	case sql.CheckAlterableTable:
		return t, nil
	case sql.TableWrapper:
		return getCheckAlterableTable(t.Underlying())
	case *ResolvedTable:
		return getCheckAlterableTable(t.Table)
	default:
		return nil, ErrNoCheckConstraintSupport.New(t.Name())
	}
}

// Expressions implements the sql.Expressioner interface.
func (c *CreateCheck) Expressions() []sql.Expression {
	return []sql.Expression{c.Check.Expr}
}

// Resolved implements the Resolvable interface.
func (c *CreateCheck) Resolved() bool {
	return c.Child.Resolved() && c.Check.Expr.Resolved()
}

// WithExpressions implements the sql.Expressioner interface.
func (c *CreateCheck) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, fmt.Errorf("expected one expression, got: %d", len(exprs))
	}

	nc := *c
	nc.Check.Expr = exprs[0]
	return &nc, nil
}

// Execute inserts the rows in the database.
func (c *CreateCheck) Execute(ctx *sql.Context) error {
	chAlterable, err := getCheckAlterable(c.UnaryNode.Child)
	if err != nil {
		return err
	}

	// check existing rows in table
	var res interface{}
	rowIter, err := c.UnaryNode.Child.RowIter(ctx, nil)
	if err != nil {
		return err
	}

	for {
		row, err := rowIter.Next()
		if row == nil || err != io.EOF {
			break
		}

		res, err = sql.EvaluateCondition(ctx, c.Check.Expr, row)
		if err != nil {
			return err
		}

		if sql.IsFalse(res) {
			return ErrCheckFailed.New(c.Check.Name)
		}
	}

	check, err := NewCheckDefinition(ctx, c.Check)
	if err != nil {
		return err
	}

	return chAlterable.CreateCheck(ctx, check)
}

// WithChildren implements the Node interface.
func (c *CreateCheck) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewAlterAddCheck(children[0], c.Check), nil
}

func (c *CreateCheck) Schema() sql.Schema { return nil }

func (c *CreateCheck) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	err := c.Execute(ctx)
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (c CreateCheck) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("AddCheck(%s)", c.Check.Name)
	_ = pr.WriteChildren(
		fmt.Sprintf("Table(%s)", c.UnaryNode.Child.String()),
		fmt.Sprintf("Expr(%s)", c.Check.Expr.String()),
	)
	return pr.String()
}

// Execute inserts the rows in the database.
func (p *DropCheck) Execute(ctx *sql.Context) error {
	chAlterable, err := getCheckAlterable(p.UnaryNode.Child)
	if err != nil {
		return err
	}
	return chAlterable.DropCheck(ctx, p.Name)
}

// RowIter implements the Node interface.
func (p *DropCheck) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	err := p.Execute(ctx)
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// WithChildren implements the Node interface.
func (p *DropCheck) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewAlterDropCheck(children[0], p.Name), nil
}
func (p *DropCheck) Schema() sql.Schema { return nil }

func (p DropCheck) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("DropCheck(%s)", p.Name)
	_ = pr.WriteChildren(fmt.Sprintf("Table(%s)", p.UnaryNode.Child.String()))
	return pr.String()
}

func NewCheckDefinition(ctx *sql.Context, check *sql.CheckConstraint) (*sql.CheckDefinition, error) {
	// When transforming an analyzed CheckConstraint into a CheckDefinition (for storage), we strip off any table
	// qualifiers that got resolved during analysis. This is to naively match the MySQL behavior, which doesn't print
	// any table qualifiers in check expressions.
	unqualifiedCols, err := expression.TransformUp(ctx, check.Expr, func(e sql.Expression) (sql.Expression, error) {
		gf, ok := e.(*expression.GetField)
		if ok {
			return expression.NewGetField(gf.Index(), gf.Type(), gf.Name(), gf.IsNullable()), nil
		}
		return e, nil
	})
	if err != nil {
		return nil, err
	}

	return &sql.CheckDefinition{
		Name:            check.Name,
		CheckExpression: fmt.Sprintf("%s", unqualifiedCols),
		Enforced:        check.Enforced,
	}, nil
}

// DropConstraint is a temporary node to handle dropping a named constraint on a table. The type of the constraint is
// not known, and is determined during analysis.
type DropConstraint struct {
	UnaryNode
	Name string
}

func (d *DropConstraint) String() string {
	tp := sql.NewTreePrinter()
	_ = tp.WriteNode("DropConstraint(%s)", d.Name)
	_ = tp.WriteChildren(d.UnaryNode.Child.String())
	return tp.String()
}

func (d *DropConstraint) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	panic("DropConstraint is a placeholder node, but RowIter was called")
}

func (d DropConstraint) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}

	nd := &d
	nd.UnaryNode = UnaryNode{children[0]}
	return nd, nil
}

// NewDropConstraint returns a new DropConstraint node
func NewDropConstraint(table *UnresolvedTable, name string) *DropConstraint {
	return &DropConstraint{
		UnaryNode: UnaryNode{table},
		Name:      name,
	}
}
