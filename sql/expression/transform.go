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

package expression

import (
	"github.com/dolthub/go-mysql-server/sql"
)

// TransformExprWithNodeFunc is a function that given an expression and the node that contains it, will return that
// expression as is or transformed along with an error, if any.
type TransformExprWithNodeFunc func(sql.Node, sql.Expression) (sql.Expression, error)

// TransformUp applies a transformation function to the given expression from the
// bottom up.
func TransformUp(e sql.Expression, f sql.TransformExprFunc) (sql.Expression, error) {
	children := e.Children()
	if len(children) == 0 {
		e, err := e.WithChildren()
		if err != nil {
			return nil, err
		}
		return f(e)
	}

	newChildren := make([]sql.Expression, len(children))
	for i, c := range children {
		c, err := TransformUp(c, f)
		if err != nil {
			return nil, err
		}
		newChildren[i] = c
	}

	e, err := e.WithChildren(newChildren...)
	if err != nil {
		return nil, err
	}

	return f(e)
}

// Clone duplicates an existing sql.Expression, returning new nodes with the
// same structure and internal values. It can be useful when dealing with
// stateful expression nodes where an evaluation needs to create multiple
// independent histories of the internal state of the expression nodes.
func Clone(expr sql.Expression) (sql.Expression, error) {
	return TransformUp(expr, func(e sql.Expression) (sql.Expression, error) {
		return e, nil
	})
}

// TransformUpWithNode applies a transformation function to the given expression from the bottom up.
func TransformUpWithNode(n sql.Node, e sql.Expression, f TransformExprWithNodeFunc) (sql.Expression, error) {
	children := e.Children()
	if len(children) == 0 {
		e, err := e.WithChildren()
		if err != nil {
			return nil, err
		}
		return f(n, e)
	}

	newChildren := make([]sql.Expression, len(children))
	for i, c := range children {
		c, err := TransformUpWithNode(n, c, f)
		if err != nil {
			return nil, err
		}
		newChildren[i] = c
	}

	e, err := e.WithChildren(newChildren...)
	if err != nil {
		return nil, err
	}

	return f(n, e)
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
