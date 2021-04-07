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
	"strings"
)

// A Window specifies the window parameters of a window function
type Window struct {
	PartitionBy []Expression
	OrderBy     SortFields
	// TODO: window frame
}

func NewWindow(partitionBy []Expression, orderBy []SortField) *Window {
	return &Window{PartitionBy: partitionBy, OrderBy: orderBy}
}

// ToExpressions converts the PartitionBy and OrderBy expressions to a single slice of expressions suitable for
// manipulation by analyzer rules.
func (w *Window) ToExpressions() []Expression {
	if w == nil {
		return nil
	}
	return append(w.OrderBy.ToExpressions(), w.PartitionBy...)
}

// FromExpressions returns copy of this window with the given expressions taken to stand in for the partition and order
// by fields. An error is returned if the lengths or types of these expressions are incompatible with this window.
func (w *Window) FromExpressions(children []Expression) (*Window, error) {
	if w == nil {
		return nil, nil
	}

	if len(children) != len(w.OrderBy)+len(w.PartitionBy) {
		return nil, ErrInvalidChildrenNumber.New(w, len(children), len(w.OrderBy)+len(w.PartitionBy))
	}

	nw := *w
	nw.OrderBy = nw.OrderBy.FromExpressions(children[:len(nw.OrderBy)])
	nw.PartitionBy = children[len(nw.OrderBy):]
	return &nw, nil
}

func (w *Window) String() string {
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
				sb.WriteString(expression.String())
			}
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
	sb.WriteString(")")
	return sb.String()
}

func (w *Window) DebugString() string {
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
			sb.WriteString(DebugString(expression))
		}
	}
	if len(w.OrderBy) > 0 {
		sb.WriteString(" order by ")
		for i, ob := range w.OrderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(DebugString(ob))
		}
	}
	sb.WriteString(")")
	return sb.String()
}
