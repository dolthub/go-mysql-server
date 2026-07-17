// Copyright 2026 Dolthub, Inc.
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

	"gopkg.in/src-d/go-errors.v1"
)

// SortCondition defines an Expression and ordering by which a query will be sorted.
type SortCondition struct {
	// Expr to order by.
	Expr Expression
	// Order type.
	Order SortOrder
	// NullOrdering defining how nulls will be ordered.
	NullOrdering NullOrdering
}

type SortConditions []SortCondition

func (sc SortConditions) ToExpressions() []Expression {
	es := make([]Expression, len(sc))
	for i, f := range sc {
		es[i] = f.Expr
	}
	return es
}

func (sc SortConditions) FromExpressions(ctx *Context, exprs ...Expression) SortConditions {
	var conds = make(SortConditions, len(sc))

	if len(exprs) != len(conds) {
		panic(fmt.Sprintf("Invalid expression slice. Wanted %d elements, got %d", len(conds), len(exprs)))
	}

	for i, expr := range exprs {
		conds[i] = SortCondition{
			Expr:         expr,
			NullOrdering: sc[i].NullOrdering,
			Order:        sc[i].Order,
		}
	}
	return conds
}

func (s SortCondition) String() string {
	return fmt.Sprintf("%s %s", s.Expr, s.Order)
}

func (s SortCondition) DebugString(ctx *Context) string {
	nullOrdering := "nullsFirst"
	if s.NullOrdering == NullsLast {
		nullOrdering = "nullsLast"
	}
	return fmt.Sprintf("%s %s %s", DebugString(ctx, s.Expr), DebugString(ctx, s.Order), nullOrdering)
}

// ErrUnableSort is thrown when something happens on sorting
var ErrUnableSort = errors.NewKind("unable to sort")

// SortOrder represents the order of the sort (ascending or descending).
type SortOrder byte

const (
	// Ascending order.
	Ascending SortOrder = 1
	// Descending order.
	Descending SortOrder = 2
)

func (s SortOrder) String() string {
	switch s {
	case Ascending:
		return "ASC"
	case Descending:
		return "DESC"
	default:
		return "invalid SortOrder"
	}
}

// NullOrdering represents how to order based on null values.
type NullOrdering byte

const (
	// NullsFirst puts the null values before any other values.
	NullsFirst NullOrdering = iota
	// NullsLast puts the null values after all other values.
	NullsLast NullOrdering = 2
)
