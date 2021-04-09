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
		Name: name,
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
	ok := true
	sql.Inspect(c.Check.Expr, func(expr sql.Expression) bool {
		switch expr.(type) {
		case *expression.UnresolvedColumn:
			ok = false
			return false
		}
		return true
	})
	return ok
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
func (p *CreateCheck) Execute(ctx *sql.Context) error {
	chAlterable, err := getCheckAlterable(p.UnaryNode.Child)
	if err != nil {
		return err
	}

	// check existing rows in table
	var res interface{}
	rowIter, err := p.UnaryNode.Child.RowIter(ctx, nil)
	if err != nil {
		return err
	}

	for {
		row, err := rowIter.Next()
		if row == nil || err != io.EOF {
			break
		}
		res, err = p.Check.Expr.Eval(ctx, row)
		if err != nil {
			return err
		}
		if val, ok := res.(bool); !ok || !val {
			return ErrCheckFailed.New(p.Check.Name)
		}
	}

	check, err := NewCheckDefinition(p.Check)
	if err != nil {
		return err
	}

	return chAlterable.CreateCheck(ctx, check)
}

// WithChildren implements the Node interface.
func (p *CreateCheck) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewAlterAddCheck(children[0], p.Check), nil
}

func (p *CreateCheck) Schema() sql.Schema { return nil }

func (p *CreateCheck) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	err := p.Execute(ctx)
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (p CreateCheck) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("AddCheck(%s)", p.Check.Name)
	_ = pr.WriteChildren(
		fmt.Sprintf("Table(%s)", p.UnaryNode.Child.String()),
		fmt.Sprintf("Expr(%s)", p.Check.Expr.String()),
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

func NewCheckDefinition(check *sql.CheckConstraint) (*sql.CheckDefinition, error) {
	return &sql.CheckDefinition{
		Name:            check.Name,
		CheckExpression: fmt.Sprintf("%s", check.Expr),
		Enforced:        check.Enforced,
	}, nil
}
