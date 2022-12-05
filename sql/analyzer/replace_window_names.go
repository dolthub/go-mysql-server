// Copyright 2022 DoltHub, Inc.
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

package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// replaceNamedWindows will 1) extract window definitions from a *plan.NamedWindows node,
// 2) resolve window name references, 3) embed resolved window definitions in sql.Window clauses
// (currently in expression.UnresolvedFunction instances), and 4) replace the plan.NamedWindows
// node with its child *plan.Window.
func replaceNamedWindows(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n.(type) {
		case *plan.NamedWindows:
			wn, ok := n.(*plan.NamedWindows)
			if !ok {
				return n, transform.SameTree, nil
			}

			window, ok := wn.Child.(*plan.Window)
			if !ok {
				return n, transform.SameTree, nil
			}

			err := checkCircularWindowDef(wn.WindowDefs)
			if err != nil {
				return nil, transform.SameTree, err
			}

			// find and replace over expressions with new window definitions
			// over sql.Windows are in unresolved aggregation functions
			newExprs := make([]sql.Expression, len(window.SelectExprs))
			same := transform.SameTree
			for i, expr := range window.SelectExprs {
				newExprs[i], _, err = transform.Expr(expr, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
					uf, ok := e.(*expression.UnresolvedFunction)
					if !ok {
						return e, transform.SameTree, nil
					}
					if uf.Window == nil {
						return e, transform.SameTree, nil
					}
					newWindow, sameDef, err := resolveWindowDef(uf.Window, wn.WindowDefs)
					if err != nil {
						return nil, transform.SameTree, err
					}
					same = same && sameDef
					if sameDef {
						return expr, transform.SameTree, nil
					}
					return uf.WithWindow(newWindow), transform.NewTree, nil
				})
				if err != nil {
					return nil, transform.SameTree, err
				}
			}
			if same {
				return window, transform.SameTree, nil
			}
			return plan.NewWindow(newExprs, window.Child), transform.NewTree, nil
		}
		return n, transform.SameTree, nil
	})
}

// checkCircularWindowDef verifies that window references terminate
// with concrete definitions. We use a linked-list algorithm
// because a sql.WindowDefinition can have at most one [Ref].
func checkCircularWindowDef(windowDefs map[string]*sql.WindowDefinition) error {
	var head, tail *sql.WindowDefinition
	for _, def := range windowDefs {
		if def.Ref == "" {
			continue
		}
		head = def
		head = windowDefs[head.Ref]
		tail = def
		for head != nil && tail != nil && head != tail {
			tail = windowDefs[tail.Ref]
			head = windowDefs[head.Ref]
			if head != nil {
				head = windowDefs[head.Ref]
			}
		}
		if head != nil && head == tail {
			return sql.ErrCircularWindowInheritance.New()
		}
	}
	return nil
}

// resolveWindowDef uses DFS to walk the [windowDefs] adjacency list, resolving and merging
// all named windows required to define the topmost window of concern.
// A WindowDefinition is considered resolved when its [Ref] is empty. Otherwise, we recurse
// to define that Ref'd window, before finally merging the resolved ref with the original window
// definition.
// A sql.WindowDef can have at most one named reference.
// We cache merged definitions in [windowDefs] to aid subsequent lookups.
func resolveWindowDef(n *sql.WindowDefinition, windowDefs map[string]*sql.WindowDefinition) (*sql.WindowDefinition, transform.TreeIdentity, error) {
	// base case
	if n.Ref == "" {
		return n, transform.SameTree, nil
	}

	var err error
	ref, ok := windowDefs[n.Ref]
	if !ok {
		return nil, transform.SameTree, sql.ErrUnknownWindowName.New(n.Ref)
	}

	// recursively resolve [n.Ref]
	ref, _, err = resolveWindowDef(ref, windowDefs)
	if err != nil {
		return nil, transform.SameTree, err
	}

	// [n] is fully defined by its attributes merging with the named reference
	n, err = mergeWindowDefs(n, ref)
	if err != nil {
		return nil, transform.SameTree, err
	}

	if n.Name != "" {
		// cache lookup
		windowDefs[n.Name] = n
	}
	return n, transform.NewTree, nil
}

// mergeWindowDefs combines the attributes of two window definitions or returns
// an error if the two are incompatible. [def] should have a reference to
// [ref] through [def.Ref], and the return value drops the reference to indicate
// the two were properly combined.
func mergeWindowDefs(def, ref *sql.WindowDefinition) (*sql.WindowDefinition, error) {
	if ref.Ref != "" {
		panic("unreachable; cannot merge unresolved window definition")
	}

	var orderBy sql.SortFields
	switch {
	case len(def.OrderBy) > 0 && len(ref.OrderBy) > 0:
		return nil, sql.ErrInvalidWindowInheritance.New("", "", "both contain order by clause")
	case len(def.OrderBy) > 0:
		orderBy = def.OrderBy
	case len(ref.OrderBy) > 0:
		orderBy = ref.OrderBy
	default:
	}

	var partitionBy []sql.Expression
	switch {
	case len(def.PartitionBy) > 0 && len(ref.PartitionBy) > 0:
		return nil, sql.ErrInvalidWindowInheritance.New("", "", "both contain partition by clause")
	case len(def.PartitionBy) > 0:
		partitionBy = def.PartitionBy
	case len(ref.PartitionBy) > 0:
		partitionBy = ref.PartitionBy
	default:
		partitionBy = []sql.Expression{}
	}

	var frame sql.WindowFrame
	switch {
	case def.Frame != nil && ref.Frame != nil:
		_, isDefDefaultFrame := def.Frame.(*plan.RowsUnboundedPrecedingToUnboundedFollowingFrame)
		_, isRefDefaultFrame := ref.Frame.(*plan.RowsUnboundedPrecedingToUnboundedFollowingFrame)

		// if both frames are set and one is RowsUnboundedPrecedingToUnboundedFollowingFrame (default),
		// we should use the other frame
		if isDefDefaultFrame {
			frame = ref.Frame
		} else if isRefDefaultFrame {
			frame = def.Frame
		} else {
			// if both frames have identical string representations, use either one
			df := def.Frame.String()
			rf := ref.Frame.String()
			if df != rf {
				return nil, sql.ErrInvalidWindowInheritance.New("", "", "both contain different frame clauses")
			}
			frame = def.Frame
		}
	case def.Frame != nil:
		frame = def.Frame
	case ref.Frame != nil:
		frame = ref.Frame
	default:
	}

	return sql.NewWindowDefinition(partitionBy, orderBy, frame, "", def.Name), nil
}
