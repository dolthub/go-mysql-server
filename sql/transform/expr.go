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

package transform

import (
	"errors"

	"github.com/dolthub/go-mysql-server/sql"
)

// Expr applies a transformation function to the given expression
// tree from the bottom up. Each callback [f] returns a TreeIdentity
// that is aggregated into a final output indicating whether the
// expression tree was changed.
func Expr(e sql.Expression, f ExprFunc) (sql.Expression, TreeIdentity, error) {
	children := e.Children()
	if len(children) == 0 {
		return f(e)
	}

	var (
		newChildren []sql.Expression
		err         error
	)

	for i := 0; i < len(children); i++ {
		c := children[i]
		c, same, err := Expr(c, f)
		if err != nil {
			return nil, SameTree, err
		}
		if !same {
			if newChildren == nil {
				newChildren = make([]sql.Expression, len(children))
				copy(newChildren, children)
			}
			newChildren[i] = c
		}
	}

	sameC := SameTree
	if len(newChildren) > 0 {
		sameC = NewTree
		e, err = e.WithChildren(newChildren...)
		if err != nil {
			return nil, SameTree, err
		}
	}

	e, sameN, err := f(e)
	if err != nil {
		return nil, SameTree, err
	}
	return e, sameC && sameN, nil
}

// InspectExpr traverses the given expression tree from the bottom up, breaking if
// stop = true. Returns a bool indicating whether traversal was interrupted.
func InspectExpr(node sql.Expression, f func(sql.Expression) bool) bool {
	stop := errors.New("stop")
	_, _, err := Expr(node, func(e sql.Expression) (sql.Expression, TreeIdentity, error) {
		ok := f(e)
		if ok {
			return nil, SameTree, stop
		}
		return e, SameTree, nil
	})
	return errors.Is(err, stop)
}

// Clone duplicates an existing sql.Expression, returning new nodes with the
// same structure and internal values. It can be useful when dealing with
// stateful expression nodes where an evaluation needs to create multiple
// independent histories of the internal state of the expression nodes.
func Clone(expr sql.Expression) (sql.Expression, error) {
	expr, _, err := Expr(expr, func(e sql.Expression) (sql.Expression, TreeIdentity, error) {
		return e, NewTree, nil
	})
	return expr, err
}

// ExprWithNode applies a transformation function to the given expression from the bottom up.
func ExprWithNode(n sql.Node, e sql.Expression, f ExprWithNodeFunc) (sql.Expression, TreeIdentity, error) {
	children := e.Children()
	if len(children) == 0 {
		return f(n, e)
	}

	var (
		newChildren []sql.Expression
		err         error
	)

	for i := 0; i < len(children); i++ {
		c := children[i]
		c, sameC, err := ExprWithNode(n, c, f)
		if err != nil {
			return nil, SameTree, err
		}
		if !sameC {
			if newChildren == nil {
				newChildren = make([]sql.Expression, len(children))
				copy(newChildren, children)
			}
			newChildren[i] = c
		}
	}

	sameC := SameTree
	if len(newChildren) > 0 {
		sameC = NewTree
		e, err = e.WithChildren(newChildren...)
		if err != nil {
			return nil, SameTree, err
		}
	}

	e, sameN, err := f(n, e)
	if err != nil {
		return nil, SameTree, err
	}
	return e, sameC && sameN, nil
}

// ExpressionToColumn converts the expression to the form that should be used in a Schema. Expressions that have Name()
// and Table() methods will use these; otherwise, String() and "" are used, respectively. The type and nullability are
// taken from the expression directly.
func ExpressionToColumn(e sql.Expression) *sql.Column {
	var name string
	if n, ok := e.(sql.Nameable); ok {
		name = n.Name()
	} else {
		name = e.String()
	}

	var table string
	if t, ok := e.(sql.Tableable); ok {
		table = t.Table()
	}

	return &sql.Column{
		Name:     name,
		Type:     e.Type(),
		Nullable: e.IsNullable(),
		Source:   table,
	}
}
