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

package plan

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

//go:generate stringer -type=JoinType -linecomment

type JoinType uint16

const (
	JoinTypeUnknown         JoinType = iota // UnknownJoin
	JoinTypeCross                           // CrossJoin
	JoinTypeInner                           // InnerJoin
	JoinTypeSemi                            // SemiJoin
	JoinTypeAnti                            // AntiJoin
	JoinTypeRightSemi                       // RightSemiJoin
	JoinTypeLeftOuter                       // LeftOuterJoin
	JoinTypeFullOuter                       // FullOuterJoin
	JoinTypeGroupBy                         // GroupByJoin
	JoinTypeRightOuter                      // RightJoin
	JoinTypeLookup                          // LookupJoin
	JoinTypeLeftOuterLookup                 // LeftOuterLookupJoin
	JoinTypeHash                            // HashJoin
	JoinTypeLeftOuterHash                   // LeftOuterHashJoin
	JoinTypeMerge                           // MergeJoin
	JoinTypeLeftOuterMerge                  // LeftOuterMergeJoin
	JoinTypeSemiHash                        // SemiHashJoin
	JoinTypeAntiHash                        // AntiHashJoin
	JoinTypeSemiLookup                      // SemiLookupJoin
	JoinTypeAntiLookup                      // AntiLookupJoin
	JoinTypeRightSemiLookup                 // RightSemiLookupJoin
	JoinTypeSemiMerge                       // SemiMergeJoin
	JoinTypeAntiMerge                       // AntiMergeJoin
	JoinTypeNatural                         // NaturalJoin
)

func (i JoinType) IsLeftOuter() bool {
	switch i {
	case JoinTypeLeftOuter, JoinTypeLeftOuterLookup, JoinTypeLeftOuterHash, JoinTypeLeftOuterMerge:
		return true
	default:
		return false
	}
}

func (i JoinType) IsRightOuter() bool {
	return i == JoinTypeRightOuter
}

func (i JoinType) IsFullOuter() bool {
	return i == JoinTypeFullOuter
}

func (i JoinType) IsPhysical() bool {
	switch i {
	case JoinTypeLookup, JoinTypeLeftOuterLookup,
		JoinTypeSemiLookup, JoinTypeRightSemiLookup, JoinTypeSemiMerge, JoinTypeSemiHash,
		JoinTypeHash, JoinTypeLeftOuterHash,
		JoinTypeMerge, JoinTypeLeftOuterMerge,
		JoinTypeAntiLookup, JoinTypeAntiMerge, JoinTypeAntiHash:
		return true
	default:
		return false
	}
}

func (i JoinType) IsInner() bool {
	switch i {
	case JoinTypeInner, JoinTypeCross:
		return true
	default:
		return false
	}
}

func (i JoinType) IsNatural() bool {
	return i == JoinTypeNatural
}

func (i JoinType) IsDegenerate() bool {
	return i == JoinTypeCross
}

func (i JoinType) IsMerge() bool {
	switch i {
	case JoinTypeMerge, JoinTypeSemiMerge, JoinTypeAntiMerge, JoinTypeLeftOuterMerge:
		return true
	default:
		return false
	}
}

func (i JoinType) IsHash() bool {
	switch i {
	case JoinTypeHash, JoinTypeSemiHash, JoinTypeAntiHash, JoinTypeLeftOuterHash:
		return true
	default:
		return false
	}
}

func (i JoinType) IsRightPartial() bool {
	switch i {
	case JoinTypeRightSemi, JoinTypeRightSemiLookup:
		return true
	default:

		return false
	}
}

func (i JoinType) IsSemi() bool {
	switch i {
	case JoinTypeRightSemi, JoinTypeSemi, JoinTypeSemiLookup, JoinTypeSemiMerge, JoinTypeSemiHash, JoinTypeRightSemiLookup:
		return true
	default:

		return false
	}
}

func (i JoinType) IsAnti() bool {
	switch i {
	case JoinTypeAnti, JoinTypeAntiLookup, JoinTypeAntiMerge, JoinTypeAntiHash:
		return true
	default:
		return false
	}
}

func (i JoinType) IsPartial() bool {
	return i == JoinTypeSemi ||
		i == JoinTypeAnti ||
		i == JoinTypeRightSemi ||
		i == JoinTypeSemiHash ||
		i == JoinTypeAntiHash ||
		i == JoinTypeAntiLookup ||
		i == JoinTypeSemiLookup ||
		i == JoinTypeRightSemiLookup
}

func (i JoinType) IsPlaceholder() bool {
	return i == JoinTypeRightOuter ||
		i == JoinTypeNatural
}

func (i JoinType) IsLookup() bool {
	switch i {
	case JoinTypeLookup, JoinTypeLeftOuterLookup, JoinTypeAntiLookup:
		return true
	default:
		return false
	}
}

func (i JoinType) IsCross() bool {
	return i == JoinTypeCross
}

func (i JoinType) AsHash() JoinType {
	switch i {
	case JoinTypeInner:
		return JoinTypeHash
	case JoinTypeLeftOuter:
		return JoinTypeLeftOuterHash
	case JoinTypeSemi:
		return JoinTypeSemiHash
	case JoinTypeAnti:
		return JoinTypeAntiHash
	default:
		return i
	}
}

func (i JoinType) AsMerge() JoinType {
	switch i {
	case JoinTypeInner:
		return JoinTypeMerge
	case JoinTypeLeftOuter:
		return JoinTypeLeftOuterMerge
	case JoinTypeSemi:
		return JoinTypeSemiMerge
	case JoinTypeAnti:
		return JoinTypeAntiMerge
	default:
		return i
	}
}

func (i JoinType) AsLookup() JoinType {
	switch i {
	case JoinTypeInner:
		return JoinTypeLookup
	case JoinTypeLeftOuter:
		return JoinTypeLeftOuterLookup
	case JoinTypeSemi:
		return JoinTypeSemiLookup
	case JoinTypeAnti:
		return JoinTypeAntiLookup
	case JoinTypeRightSemi:
		return JoinTypeRightSemiLookup
	default:
		return i
	}
}

// JoinNode contains all the common data fields and implements the common sql.Node getters for all join types.
type JoinNode struct {
	BinaryNode
	Filter     sql.Expression
	Op         JoinType
	CommentStr string
	ScopeLen   int
}

var _ sql.Node = (*JoinNode)(nil)
var _ sql.CollationCoercible = (*JoinNode)(nil)

func NewJoin(left, right sql.Node, op JoinType, cond sql.Expression) *JoinNode {
	return &JoinNode{
		Op:         op,
		BinaryNode: BinaryNode{left: left, right: right},
		Filter:     cond,
	}
}

// Expressions implements sql.Expression
func (j *JoinNode) Expressions() []sql.Expression {
	if j.Op.IsDegenerate() || j.Filter == nil {
		return nil
	}
	return []sql.Expression{j.Filter}
}

func (j *JoinNode) JoinCond() sql.Expression {
	return j.Filter
}

// Comment implements sql.CommentedNode
func (j *JoinNode) Comment() string {
	return j.CommentStr
}

// Resolved implements the Resolvable interface.
func (j *JoinNode) Resolved() bool {
	switch {
	case j.Op.IsNatural():
		return false
	case j.Op.IsDegenerate() || j.Filter == nil:
		return j.left.Resolved() && j.right.Resolved()
	default:
		return j.left.Resolved() && j.right.Resolved() && j.Filter.Resolved()
	}
}

func (j *JoinNode) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	ret := *j
	switch {
	case j.Op.IsDegenerate() || j.Filter == nil:
		if len(exprs) != 0 {
			return nil, sql.ErrInvalidChildrenNumber.New(j, len(exprs), 0)
		}
	default:
		if len(exprs) != 1 {
			return nil, sql.ErrInvalidChildrenNumber.New(j, len(exprs), 1)
		}
		ret.Filter = exprs[0]
	}
	return &ret, nil
}

func (j *JoinNode) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return j.left.CheckPrivileges(ctx, opChecker) && j.right.CheckPrivileges(ctx, opChecker)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*JoinNode) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	// Joins make use of coercibility, but they don't return anything themselves
	return sql.Collation_binary, 7
}

func (j *JoinNode) JoinType() JoinType {
	return j.Op
}

// Schema implements the Node interface.
func (j *JoinNode) Schema() sql.Schema {
	switch {
	case j.Op.IsLeftOuter():
		return append(j.left.Schema(), makeNullable(j.right.Schema())...)
	case j.Op.IsRightOuter():
		return append(makeNullable(j.left.Schema()), j.right.Schema()...)
	case j.Op.IsFullOuter():
		return append(makeNullable(j.left.Schema()), makeNullable(j.right.Schema())...)
	case j.Op.IsRightPartial():
		return j.Right().Schema()
	case j.Op.IsPartial():
		return j.Left().Schema()
	case j.Op.IsNatural():
		panic("NaturalJoin is a placeholder, Schema called")
	default:
		return append(j.left.Schema(), j.right.Schema()...)
	}
}

// makeNullable will return a copy of the received columns, but all of them
// will be turned into nullable columns.
func makeNullable(cols []*sql.Column) []*sql.Column {
	var result = make([]*sql.Column, len(cols))
	for i := 0; i < len(cols); i++ {
		col := *cols[i]
		col.Nullable = true
		result[i] = &col
	}
	return result
}

func (j *JoinNode) WithScopeLen(i int) *JoinNode {
	ret := *j
	ret.ScopeLen = i
	return &ret
}

func (j *JoinNode) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(children), 2)
	}
	ret := *j
	ret.left = children[0]
	ret.right = children[1]
	return &ret, nil
}

// WithComment implements sql.CommentedNode
func (j *JoinNode) WithComment(comment string) sql.Node {
	ret := *j
	ret.CommentStr = comment
	return &ret
}

func (j *JoinNode) String() string {
	pr := sql.NewTreePrinter()
	var children []string
	if j.Filter != nil {
		if j.Op.IsMerge() {
			filters := expression.SplitConjunction(j.Filter)
			children = append(children, fmt.Sprintf("cmp: %s", filters[0]))
			if len(filters) > 1 {
				children = append(children, fmt.Sprintf("sel: %s", expression.JoinAnd(filters[1:]...)))
			}
		} else {
			children = append(children, j.Filter.String())
		}
	}
	children = append(children, j.left.String(), j.right.String())
	pr.WriteNode("%s", j.Op)
	pr.WriteChildren(children...)
	return pr.String()
}

func (j *JoinNode) DebugString() string {
	pr := sql.NewTreePrinter()
	var children []string
	if j.Filter != nil {
		if j.Op.IsMerge() {
			filters := expression.SplitConjunction(j.Filter)
			children = append(children, fmt.Sprintf("cmp: %s", sql.DebugString(filters[0])))
			if len(filters) > 1 {
				children = append(children, fmt.Sprintf("sel: %s", sql.DebugString(expression.JoinAnd(filters[1:]...))))
			}
		} else {
			children = append(children, sql.DebugString(j.Filter))
		}
	}
	children = append(children, sql.DebugString(j.left), sql.DebugString(j.right))
	pr.WriteNode("%s", j.Op)
	pr.WriteChildren(children...)
	return pr.String()
}

func NewInnerJoin(left, right sql.Node, cond sql.Expression) *JoinNode {
	return NewJoin(left, right, JoinTypeInner, cond)
}

func NewHashJoin(left, right sql.Node, cond sql.Expression) *JoinNode {
	return NewJoin(left, right, JoinTypeHash, cond)
}

func NewLeftOuterJoin(left, right sql.Node, cond sql.Expression) *JoinNode {
	return NewJoin(left, right, JoinTypeLeftOuter, cond)
}

func NewLeftOuterHashJoin(left, right sql.Node, cond sql.Expression) *JoinNode {
	return NewJoin(left, right, JoinTypeLeftOuterHash, cond)
}

func NewLeftOuterLookupJoin(left, right sql.Node, cond sql.Expression) *JoinNode {
	return NewJoin(left, right, JoinTypeLeftOuterLookup, cond)
}

func NewRightOuterJoin(left, right sql.Node, cond sql.Expression) *JoinNode {
	return NewJoin(left, right, JoinTypeRightOuter, cond)
}

func NewFullOuterJoin(left, right sql.Node, cond sql.Expression) *JoinNode {
	return NewJoin(left, right, JoinTypeFullOuter, cond)
}

func NewCrossJoin(left, right sql.Node) *JoinNode {
	return NewJoin(left, right, JoinTypeCross, nil)
}

// NaturalJoin is a join that automatically joins by all the columns with the
// same name.
// NaturalJoin is a placeholder node, it should be transformed into an INNER
// JOIN during analysis.
func NewNaturalJoin(left, right sql.Node) *JoinNode {
	return NewJoin(left, right, JoinTypeNatural, nil)
}

// An LookupJoin is a join that uses index lookups for the secondary table.
func NewLookupJoin(left, right sql.Node, cond sql.Expression) *JoinNode {
	return NewJoin(left, right, JoinTypeLookup, cond)
}

func NewAntiJoin(left, right sql.Node, cond sql.Expression) *JoinNode {
	return NewJoin(left, right, JoinTypeAnti, cond)
}

func NewSemiJoin(left, right sql.Node, cond sql.Expression) *JoinNode {
	return NewJoin(left, right, JoinTypeSemi, cond)
}

// IsNullRejecting returns whether the expression always returns false for
// nil inputs.
func IsNullRejecting(e sql.Expression) bool {
	return !transform.InspectExpr(e, func(e sql.Expression) bool {
		switch e.(type) {
		case *expression.NullSafeEquals, *expression.IsNull:
			return true
		default:
			return false
		}
	})
}
