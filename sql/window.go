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

package sql

import (
	"fmt"
	"strings"

	"github.com/cespare/xxhash/v2"
)

func NewWindowDefinition(partitionBy []Expression, orderBy SortConditions, frame WindowFrame, ref, name string) *WindowDefinition {
	return &WindowDefinition{
		PartitionBy: partitionBy,
		OrderBy:     orderBy,
		Frame:       frame,
		Ref:         ref,
		Name:        name,
	}
}

// A WindowDefinition specifies the window parameters of a window function
type WindowDefinition struct {
	Frame       WindowFrame
	Ref         string
	Name        string
	PartitionBy []Expression
	OrderBy     SortConditions
}

func (w *WindowDefinition) ExpressionsLen() int {
	return len(w.OrderBy) + len(w.PartitionBy)
}

// ToExpressions converts the PartitionBy and OrderBy expressions to a single slice of expressions suitable for
// manipulation by analyzer rules.
func (w *WindowDefinition) ToExpressions() []Expression {
	if w == nil {
		return nil
	}
	return append(w.OrderBy.ToExpressions(), w.PartitionBy...)
}

// FromExpressions returns copy of this window with the given expressions taken to stand in for the partition and order
// by fields. An error is returned if the lengths or types of these expressions are incompatible with this window.
func (w *WindowDefinition) FromExpressions(ctx *Context, children []Expression) (*WindowDefinition, error) {
	if w == nil {
		return nil, nil
	}

	if len(children) != w.ExpressionsLen() {
		return nil, ErrInvalidChildrenNumber.New(w, len(children), len(w.OrderBy)+len(w.PartitionBy))
	}

	nw := *w
	nw.OrderBy = nw.OrderBy.FromExpressions(ctx, children[:len(nw.OrderBy)]...)
	nw.PartitionBy = children[len(nw.OrderBy):]
	return &nw, nil
}

func (w *WindowDefinition) String() string {
	if w == nil {
		return ""
	}
	sb := strings.Builder{}
	sb.WriteString("over (")
	if len(w.PartitionBy) > 0 {
		sb.WriteString(" partition by ")
		for i, expression := range w.PartitionBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(expression.String())
		}
	}
	if len(w.OrderBy) > 0 {
		sb.WriteString(" order by ")
		for i, ob := range w.OrderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(ob.String())
		}
	}
	if w.Frame != nil {
		sb.WriteString(fmt.Sprintf(" %s", w.Frame.String()))
	}
	sb.WriteString(")")
	return sb.String()
}

// Describe implements the Describable interface. Window definitions must pass describe options to their expressions
// so that physical expression details are not lost when they are nested in an aggregate.
func (w *WindowDefinition) Describe(ctx *Context, options DescribeOptions) string {
	if w == nil {
		return ""
	}
	if options.Debug {
		return w.DebugString(ctx)
	}
	sb := strings.Builder{}
	sb.WriteString("over (")
	if len(w.PartitionBy) > 0 {
		sb.WriteString(" partition by ")
		for i, expression := range w.PartitionBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(Describe(ctx, expression, options))
		}
	}
	if len(w.OrderBy) > 0 {
		sb.WriteString(" order by ")
		for i, condition := range w.OrderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%s %s", Describe(ctx, condition.Expr, options), condition.Order))
		}
	}
	if w.Frame != nil {
		sb.WriteString(fmt.Sprintf(" %s", w.Frame.String()))
	}
	sb.WriteString(")")
	return sb.String()
}

func (w *WindowDefinition) PartitionId() (uint64, error) {
	if w == nil {
		return 0, nil
	}
	hash := xxhash.New()
	writeWindowIdentity(hash, w, false)
	return hash.Sum64(), nil
}

func (w *WindowDefinition) DebugString(ctx *Context) string {
	if w == nil {
		return ""
	}
	return w.String()
}

// WindowExpressionId returns a semantic hash of a resolved window expression.
func WindowExpressionId(expr WindowAdaptableExpression) uint64 {
	hash := xxhash.New()
	writeExpressionIdentity(hash, expr, false)
	writeWindowIdentity(hash, expr.Window(), true)
	return hash.Sum64()
}

// writeExpressionIdentity adds an expression's structure and resolved column identities to hash.
func writeExpressionIdentity(hash *xxhash.Digest, expr Expression, includeRootId bool) {
	root := true
	Inspect(nil, expr, func(_ *Context, child Expression) bool {
		_, _ = fmt.Fprintf(hash, "%T%c%s%c%d%c", child, 0, child.String(), 0, len(child.Children()), 0)
		if identified, ok := child.(IdExpression); ok && (includeRootId || !root) {
			_, _ = fmt.Fprintf(hash, "ID:%d%c", identified.Id(), 0)
		}
		root = false
		return true
	})
}

// writeWindowIdentity adds a window definition's semantic properties to hash.
func writeWindowIdentity(hash *xxhash.Digest, window *WindowDefinition, includeFrame bool) {
	if window == nil {
		_, _ = hash.WriteString("WINDOW:nil")
		return
	}
	_, _ = hash.WriteString("PARTITION:")
	for _, expression := range window.PartitionBy {
		writeExpressionIdentity(hash, expression, true)
	}
	_, _ = hash.WriteString("ORDER:")
	for _, condition := range window.OrderBy {
		writeExpressionIdentity(hash, condition.Expr, true)
		_, _ = fmt.Fprintf(hash, "ORDER:%d%cNULLS:%d%c", condition.Order, 0, condition.NullOrdering, 0)
	}
	if includeFrame {
		_, _ = hash.WriteString("FRAME:")
		if window.Frame == nil {
			_, _ = hash.WriteString("nil")
		} else {
			_, _ = hash.WriteString(window.Frame.String())
		}
	}
}
