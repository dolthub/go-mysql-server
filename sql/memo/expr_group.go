// Copyright 2023 Dolthub, Inc.
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

package memo

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// ExprGroup is a linked list of plans that return the same result set
// defined by row count and schema.
type ExprGroup struct {
	m           *Memo
	_children   []*ExprGroup
	RelProps    *relProps
	scalarProps *scalarProps
	Scalar      ScalarExpr
	First       RelExpr
	Best        RelExpr

	Id GroupId

	Cost   float64
	Done   bool
	HintOk bool
}

func newExprGroup(m *Memo, id GroupId, expr exprType) *ExprGroup {
	// bit of circularity: |grp| references |ref|, |rel| references |grp|,
	// and |relProps| references |rel| and |grp| info.
	grp := &ExprGroup{
		m:  m,
		Id: id,
	}
	expr.SetGroup(grp)
	switch e := expr.(type) {
	case RelExpr:
		grp.First = e
		grp.RelProps = newRelProps(e)
	case ScalarExpr:
		grp.Scalar = e
		grp.scalarProps = newScalarProps(e)
	}
	return grp
}

func (e *ExprGroup) ScalarProps() *scalarProps {
	return e.scalarProps
}

// Prepend adds a new plan to an expression group at the beginning of
// the list, to avoid recursive exploration steps (like adding indexed joins).
func (e *ExprGroup) Prepend(rel RelExpr) {
	first := e.First
	e.First = rel
	rel.SetNext(first)
}

// children returns a unioned list of child ExprGroup for
// every logical plan in this group.
func (e *ExprGroup) children() []*ExprGroup {
	relExpr, ok := e.First.(RelExpr)
	if !ok {
		return e.children()
	}
	n := relExpr
	children := make([]*ExprGroup, 0)
	for n != nil {
		children = append(children, n.Children()...)
		n = n.Next()
	}
	return children
}

// updateBest updates a group's Best to the given expression or a hinted
// operator if the hinted plan is not found. Join operator is applied as
// a local rather than global property.
func (e *ExprGroup) updateBest(n RelExpr, grpCost float64) {
	if e.Best == nil || grpCost <= e.Cost {
		e.Best = n
		e.Cost = grpCost
	}
}

func (e *ExprGroup) finalize(node sql.Node, input sql.Schema) (sql.Node, error) {
	props := e.RelProps
	var result = node
	if props.limit != nil {
		result = plan.NewLimit(props.limit, result)
	}
	return result, nil
}

func (e *ExprGroup) String() string {
	if e.Scalar != nil {
		return fmt.Sprintf("(%s)", FormatExpr(e.Scalar))
	}

	b := strings.Builder{}
	n := e.First
	sep := ""
	for n != nil {
		b.WriteString(sep)
		b.WriteString(fmt.Sprintf("(%s", FormatExpr(n)))
		if e.Best != nil {
			b.WriteString(fmt.Sprintf(" %.1f", n.Cost()))

			childCost := 0.0
			for _, c := range n.Children() {
				childCost += c.Cost
			}
			if e.Cost == n.Cost()+childCost {
				b.WriteString(")*")
			} else {
				b.WriteString(")")
			}
		} else {
			b.WriteString(")")
		}
		sep = " "
		n = n.Next()
	}
	return b.String()
}
