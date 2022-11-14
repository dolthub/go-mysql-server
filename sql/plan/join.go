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
	"os"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

//go:generate stringer -type=JoinType -linecomment

const (
	inMemoryJoinKey        = "INMEMORY_JOINS"
	inMemoryJoinSessionVar = "inmemory_joins"
)

var useInMemoryJoins = shouldUseMemoryJoinsByEnv()

type JoinType uint16

const (
	JoinTypeUnknown         JoinType = iota // UnknownJoin
	JoinTypeCross                           // CrossJoin
	JoinTypeInner                           // InnerJoin
	JoinTypeSemi                            // SemiJoin
	JoinTypeAnti                            // AntiJoin
	JoinTypeLeftOuter                       // LeftOuterJoin
	JoinTypeFullOuter                       // FullOuterJoin
	JoinTypeGroupBy                         // GroupByJoin
	JoinTypeRightOuter                      // RightJoin
	JoinTypeLookup                          // LookupJoin
	JoinTypeLeftOuterLookup                 // LeftOuterLookupJoin
	JoinTypeHash                            // HashJoin
	JoinTypeLeftOuterHash                   // LeftOuterHashJoin
	JoinTypeNatural                         // NaturalJoin
)

func (i JoinType) IsLeftOuter() bool {
	return i == JoinTypeLeftOuter ||
		i == JoinTypeLeftOuterLookup ||
		i == JoinTypeLeftOuterHash
}

func (i JoinType) IsRightOuter() bool {
	return i == JoinTypeRightOuter
}

func (i JoinType) IsFullOuter() bool {
	return i == JoinTypeFullOuter
}

func (i JoinType) IsPhysical() bool {
	return i == JoinTypeLookup ||
		i == JoinTypeLeftOuterLookup ||
		i == JoinTypeHash ||
		i == JoinTypeLeftOuterHash
}

func (i JoinType) IsInner() bool {
	return i == JoinTypeInner ||
		i == JoinTypeCross
}

func (i JoinType) IsNatural() bool {
	return i == JoinTypeNatural
}

func (i JoinType) IsDegenerate() bool {
	return i == JoinTypeNatural ||
		i == JoinTypeCross
}

func (i JoinType) IsPartial() bool {
	return i == JoinTypeSemi ||
		i == JoinTypeAnti
}

func (i JoinType) IsPlaceholder() bool {
	return i == JoinTypeRightOuter ||
		i == JoinTypeNatural
}

func (i JoinType) IsLookup() bool {
	return i == JoinTypeLookup ||
		i == JoinTypeLeftOuterLookup
}

func (i JoinType) IsCross() bool {
	return i == JoinTypeCross
}

func shouldUseMemoryJoinsByEnv() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(inMemoryJoinKey)))
	return v == "on" || v == "1"
}

// JoinNode contains all the common data fields and implements the commom sql.Node getters for all join types.
type JoinNode struct {
	BinaryNode
	Filter     sql.Expression
	Op         JoinType
	CommentStr string
	ScopeLen   int
}

func NewJoin(left, right sql.Node, op JoinType, cond sql.Expression) *JoinNode {
	return &JoinNode{
		Op:         op,
		BinaryNode: BinaryNode{left: left, right: right},
		Filter:     cond,
	}
}

// Expressions implements sql.Expression
func (j *JoinNode) Expressions() []sql.Expression {
	if j.Op.IsDegenerate() {
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
	case j.Op.IsDegenerate():
		return j.left.Resolved() && j.right.Resolved()
	default:
		return j.left.Resolved() && j.right.Resolved() && j.Filter.Resolved()
	}
}

func (j *JoinNode) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	ret := *j
	switch {
	case j.Op.IsDegenerate():
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

// RowIter implements the Node interface.
func (j *JoinNode) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	switch {
	case j.Op.IsFullOuter():
		return newFullJoinIter(ctx, j, row)
	case j.Op.IsPartial():
		return newExistsIter(ctx, j, row)
	case j.Op.IsCross():
		return newCrossJoinIter(ctx, j, row)
	case j.Op.IsPlaceholder():
		panic(fmt.Sprintf("%s is a placeholder, RowIter called", j.Op))
	default:
		return newJoinIter(ctx, j, row)
	}
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
	var filter string
	if j.Filter != nil {
		filter = j.Filter.String()
	}
	pr.WriteNode("%s%s", j.Op, filter)
	pr.WriteChildren(j.left.String(), j.right.String())
	return pr.String()
}

func (j *JoinNode) DebugString() string {
	pr := sql.NewTreePrinter()
	var filter string
	if j.Filter != nil {
		filter = sql.DebugString(j.Filter)
	}
	_ = pr.WriteNode("%s%s, comment=%s", j.Op, filter, j.Comment())
	_ = pr.WriteChildren(sql.DebugString(j.left), sql.DebugString(j.right))
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
