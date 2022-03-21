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

package visit

import (
	"errors"

	"github.com/dolthub/go-mysql-server/sql"
)

// TransformExprWithNodeFunc is a function that given an expression and the node that contains it, will return that
// expression as is or transformed along with an error, if any.
type TransformExprWithNodeFunc func(sql.Node, sql.Expression) (sql.Expression, sql.TreeIdentity, error)

func Exprs(e sql.Expression, f sql.TransformExprFunc) (sql.Expression, sql.TreeIdentity, error) {
	children := e.Children()
	if len(children) == 0 {
		return f(e)
	}

	var (
		newChildren []sql.Expression
		c           sql.Expression
		sameC       = sql.SameTree
		err         error
	)

	for i := 0; i < len(children); i++ {
		c = children[i]
		c, sameC, err = Exprs(c, f)
		if err != nil {
			return nil, sql.SameTree, err
		}
		if !sameC {
			if newChildren == nil {
				newChildren = make([]sql.Expression, len(children))
				copy(newChildren, children)
			}
			newChildren[i] = c
		}
	}

	if len(newChildren) > 0 {
		sameC = sql.NewTree
		e, err = e.WithChildren(newChildren...)
		if err != nil {
			return nil, sql.SameTree, err
		}
	}

	e, sameN, err := f(e)
	if err != nil {
		return nil, sql.SameTree, err
	}
	return e, sameC && sameN, nil
}

// InspectExprs traverses the given tree from the bottom up, breaking if
// stop = true. Returns a bool indicating whether traversal was interrupted.
func InspectExprs(node sql.Expression, f func(sql.Expression) bool) bool {
	stop := errors.New("stop")
	_, _, err := Exprs(node, func(e sql.Expression) (sql.Expression, sql.TreeIdentity, error) {
		ok := f(e)
		if ok {
			return nil, sql.SameTree, stop
		}
		return e, sql.SameTree, nil
	})
	return errors.Is(err, stop)
}

// Clone duplicates an existing sql.Expression, returning new nodes with the
// same structure and internal values. It can be useful when dealing with
// stateful expression nodes where an evaluation needs to create multiple
// independent histories of the internal state of the expression nodes.
func Clone(expr sql.Expression) (sql.Expression, error) {
	expr, _, err := Exprs(expr, func(e sql.Expression) (sql.Expression, sql.TreeIdentity, error) {
		return e, sql.NewTree, nil
	})
	return expr, err
}

// ExprsWithNode applies a transformation function to the given expression from the bottom up.
func ExprsWithNode(n sql.Node, e sql.Expression, f TransformExprWithNodeFunc) (sql.Expression, sql.TreeIdentity, error) {
	children := e.Children()
	if len(children) == 0 {
		return f(n, e)
	}

	var (
		newChildren []sql.Expression
		c           sql.Expression
		sameC       = sql.SameTree
		err         error
	)

	for i := 0; i < len(children); i++ {
		c = children[i]
		c, sameC, err = ExprsWithNode(n, c, f)
		if err != nil {
			return nil, sql.SameTree, err
		}
		if !sameC {
			if newChildren == nil {
				newChildren = make([]sql.Expression, len(children))
				copy(newChildren, children)
			}
			newChildren[i] = c
		}
	}

	if len(newChildren) > 0 {
		sameC = sql.NewTree
		e, err = e.WithChildren(newChildren...)
		if err != nil {
			return nil, sql.SameTree, err
		}
	}

	e, sameN, err := f(n, e)
	if err != nil {
		return nil, sql.SameTree, err
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
