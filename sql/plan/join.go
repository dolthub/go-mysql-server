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
	"errors"
	"io"
	"os"
	"reflect"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

//go:generate stringer -type=JoinType -linecomment

const (
	inMemoryJoinKey        = "INMEMORY_JOINS"
	inMemoryJoinSessionVar = "inmemory_joins"
)

var useInMemoryJoins = shouldUseMemoryJoinsByEnv()

type JoinType uint16

const (
	UnknownJoinType   JoinType = iota // UnknownJoinType
	CrossJoinType                     // CrossJoin
	InnerJoinType                     // InnerJoin
	SemiJoinType                      // SemiJoin
	AntiJoinType                      // AntiJoin
	LeftJoinType                      // LeftJoin
	FullOuterJoinType                 // FullOuterJoin
	GroupByJoinType                   // GroupByJoin
	RightJoinType                     // RightJoin
)

func shouldUseMemoryJoinsByEnv() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(inMemoryJoinKey)))
	return v == "on" || v == "1"
}

type JoinNode interface {
	sql.Node
	Left() sql.Node
	Right() sql.Node
	JoinCond() sql.Expression
	JoinType() JoinType
	Comment() string
	WithScopeLen(int) JoinNode
	WithMultipassMode() JoinNode
}

// joinStruct contains all the common data fields and implements the commom sql.Node getters for all join types.
type joinBase struct {
	BinaryNode
	Filter     sql.Expression
	Op         JoinType
	CommentStr string
	ScopeLen   int
	JoinMode   joinMode
}

// Expressions implements sql.Expression
func (j *joinBase) Expressions() []sql.Expression {
	return []sql.Expression{j.Filter}
}

func (j *joinBase) JoinCond() sql.Expression {
	return j.Filter
}

// Comment implements sql.CommentedNode
func (j *joinBase) Comment() string {
	return j.CommentStr
}

// Resolved implements the Resolvable interface.
func (j *joinBase) Resolved() bool {
	return j.left.Resolved() && j.right.Resolved() && j.Filter.Resolved()
	return j.left.Resolved() && j.right.Resolved() && j.Filter.Resolved()
}

func (j *joinBase) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return j.left.CheckPrivileges(ctx, opChecker) && j.right.CheckPrivileges(ctx, opChecker)
}
func (j *joinBase) JoinType() JoinType {
	return j.Op
}

// Schema implements the Node interface.
func (j *joinBase) Schema() sql.Schema {
	panic("join implementation should provide schema")
}

func constructNewJoin(base *joinBase) JoinNode {
	switch base.Op {
	case InnerJoinType:
		return &InnerJoin{base}
	case LeftJoinType:
		return &LeftJoin{base}
	case RightJoinType:
		return &RightJoin{base}
	case SemiJoinType:
		return &SemiJoin{&existsJoinBase{base}}
	case AntiJoinType:
		return &AntiJoin{&existsJoinBase{base}}
	case FullOuterJoinType:
		return &FullOuterJoin{base}
	default:
		panic("unexpected exist join type: %T")
	}
}
func (j *joinBase) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(exprs), 1)
	}
	ret := *j
	ret.Filter = exprs[0]
	return constructNewJoin(&ret), nil
}

func (j *joinBase) WithScopeLen(i int) JoinNode {
	ret := *j
	ret.ScopeLen = i
	return constructNewJoin(&ret)
}

func (j *joinBase) WithMultipassMode() JoinNode {
	ret := *j
	ret.JoinMode = multipassMode
	return constructNewJoin(&ret)
}

func (j *joinBase) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(children), 2)
	}
	ret := *j
	ret.left = children[0]
	ret.right = children[1]
	return constructNewJoin(&ret), nil
}

// WithComment implements sql.CommentedNode
func (j *joinBase) WithComment(comment string) sql.Node {
	ret := *j
	ret.CommentStr = comment
	return constructNewJoin(&ret)
}

func (j *joinBase) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("%s%s", j.Op, j.Filter)
	_ = pr.WriteChildren(j.left.String(), j.right.String())
	return pr.String()
}

func (j *joinBase) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("%s%s, comment=%s", j.Op, sql.DebugString(j.Filter), j.Comment())
	_ = pr.WriteChildren(sql.DebugString(j.left), sql.DebugString(j.right))
	return pr.String()
}

// InnerJoin is an inner join between two tables.
type InnerJoin struct {
	*joinBase
}

var _ JoinNode = (*InnerJoin)(nil)
var _ sql.CommentedNode = (*InnerJoin)(nil)

func (j *InnerJoin) JoinType() JoinType {
	return InnerJoinType
}

// NewInnerJoin creates a new inner join node from two tables.
func NewInnerJoin(left, right sql.Node, cond sql.Expression) *InnerJoin {
	return &InnerJoin{
		&joinBase{
			Op: InnerJoinType,
			BinaryNode: BinaryNode{
				left:  left,
				right: right,
			},
			Filter: cond,
		},
	}
}

// Schema implements the Node interface.
func (j *InnerJoin) Schema() sql.Schema {
	return append(j.left.Schema(), j.right.Schema()...)
}

// RowIter implements the Node interface.
func (j *InnerJoin) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return joinRowIter(ctx, InnerJoinType, j.left, j.right, j.Filter, row, j.ScopeLen, j.JoinMode)
}

// LeftJoin is a left join between two tables.
type LeftJoin struct {
	*joinBase
}

var _ JoinNode = (*LeftJoin)(nil)
var _ sql.CommentedNode = (*LeftJoin)(nil)

func (j *LeftJoin) JoinType() JoinType {
	return LeftJoinType
}

// NewLeftJoin creates a new left join node from two tables.
func NewLeftJoin(left, right sql.Node, cond sql.Expression) *LeftJoin {
	return &LeftJoin{
		&joinBase{
			Op: LeftJoinType,
			BinaryNode: BinaryNode{
				left:  left,
				right: right,
			},
			Filter: cond,
		},
	}
}

// Schema implements the Node interface.
func (j *LeftJoin) Schema() sql.Schema {
	return append(j.left.Schema(), makeNullable(j.right.Schema())...)
}

// RowIter implements the Node interface.
func (j *LeftJoin) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return joinRowIter(ctx, LeftJoinType, j.left, j.right, j.Filter, row, j.ScopeLen, j.JoinMode)
}

// RightJoin is a left join between two tables.
type RightJoin struct {
	*joinBase
}

func (j *RightJoin) JoinType() JoinType {
	return RightJoinType
}

var _ JoinNode = (*RightJoin)(nil)
var _ sql.CommentedNode = (*RightJoin)(nil)

// NewRightJoin creates a new right join node from two tables.
func NewRightJoin(left, right sql.Node, cond sql.Expression) *RightJoin {
	return &RightJoin{
		&joinBase{
			Op: RightJoinType,
			BinaryNode: BinaryNode{
				left:  left,
				right: right,
			},
			Filter: cond,
		},
	}
}

// Schema implements the Node interface.
func (j *RightJoin) Schema() sql.Schema {
	return append(makeNullable(j.left.Schema()), j.right.Schema()...)
}

// RowIter implements the Node interface.
func (j *RightJoin) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return joinRowIter(ctx, RightJoinType, j.left, j.right, j.Filter, row, j.ScopeLen, j.JoinMode)
}

func joinRowIter(ctx *sql.Context, typ JoinType, left, right sql.Node, cond sql.Expression, row sql.Row, scopeLen int, mode joinMode) (sql.RowIter, error) {
	var leftName, rightName string
	if leftTable, ok := left.(sql.Nameable); ok {
		leftName = leftTable.Name()
	} else {
		leftName = reflect.TypeOf(left).String()
	}

	if rightTable, ok := right.(sql.Nameable); ok {
		rightName = rightTable.Name()
	} else {
		rightName = reflect.TypeOf(right).String()
	}

	span, ctx := ctx.Span("plan."+typ.String(), trace.WithAttributes(
		attribute.String("left", leftName),
		attribute.String("right", rightName),
	))

	var inMemorySession bool
	val, err := ctx.GetSessionVariable(ctx, inMemoryJoinSessionVar)
	if err == nil && val == int8(1) {
		inMemorySession = true
	}

	if mode == unknownMode {
		if useInMemoryJoins || inMemorySession {
			mode = memoryMode
		}
	}

	cache, dispose := ctx.Memory.NewRowsCache()
	if typ == RightJoinType {
		r, err := right.RowIter(ctx, row)
		if err != nil {
			span.End()
			return nil, err
		}
		return sql.NewSpanIter(span, &joinIter{
			typ:               typ,
			primary:           r,
			secondaryProvider: left,
			cond:              cond,
			mode:              mode,
			secondaryRows:     cache,
			rowSize:           len(row) + len(left.Schema()) + len(right.Schema()),
			dispose:           dispose,
			originalRow:       row,
			scopeLen:          scopeLen,
		}), nil
	}

	l, err := left.RowIter(ctx, row)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, &joinIter{
		typ:               typ,
		primary:           l,
		secondaryProvider: right,
		cond:              cond,
		mode:              mode,
		secondaryRows:     cache,
		rowSize:           len(row) + len(left.Schema()) + len(right.Schema()),
		dispose:           dispose,
		originalRow:       row,
		scopeLen:          scopeLen,
	}), nil
}

// joinMode defines the mode in which a join will be performed.
type joinMode byte

const (
	// unknownMode is the default mode. It will start iterating without really
	// knowing in which mode it will end up computing the join. If it
	// iterates the right side fully one time and so far it fits in memory,
	// then it will switch to memory mode. Otherwise, if at some point during
	// this first iteration it finds that it does not fit in memory, will
	// switch to multipass mode.
	unknownMode joinMode = iota
	// memoryMode computes all the join directly in memory iterating each
	// side of the join exactly once.
	memoryMode
	// multipassMode computes the join by iterating the left side once,
	// and the right side one time for each row in the left side.
	multipassMode
)

// joinIter is a generic iterator for all join types.
type joinIter struct {
	typ               JoinType
	primary           sql.RowIter
	secondaryProvider rowIterProvider
	secondary         sql.RowIter
	cond              sql.Expression

	primaryRow sql.Row
	foundMatch bool
	rowSize    int

	// scope variables from outer scope
	originalRow sql.Row
	scopeLen    int

	// used to compute in-memory
	mode          joinMode
	secondaryRows sql.RowsCache
	pos           int
	dispose       sql.DisposeFunc
}

func (i *joinIter) Dispose() {
	if i.dispose != nil {
		i.dispose()
		i.dispose = nil
	}
}

func (i *joinIter) loadPrimary(ctx *sql.Context) error {
	if i.primaryRow == nil {
		r, err := i.primary.Next(ctx)
		if err != nil {
			if err == io.EOF {
				i.Dispose()
			}
			return err
		}

		i.primaryRow = i.originalRow.Append(r)
		i.foundMatch = false
	}

	return nil
}

func (i *joinIter) loadSecondaryInMemory(ctx *sql.Context) error {
	iter, err := i.secondaryProvider.RowIter(ctx, i.primaryRow)
	if err != nil {
		return err
	}

	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			iter.Close(ctx)
			return err
		}

		if err := i.secondaryRows.Add(row); err != nil {
			iter.Close(ctx)
			return err
		}
	}

	err = iter.Close(ctx)
	if err != nil {
		return err
	}

	if len(i.secondaryRows.Get()) == 0 {
		return io.EOF
	}

	return nil
}

func (i *joinIter) loadSecondary(ctx *sql.Context) (row sql.Row, err error) {
	if i.mode == memoryMode {
		if len(i.secondaryRows.Get()) == 0 {
			if err = i.loadSecondaryInMemory(ctx); err != nil {
				if err == io.EOF {
					i.primaryRow = nil
					i.pos = 0
				}
				return nil, err
			}
		}

		if i.pos >= len(i.secondaryRows.Get()) {
			i.primaryRow = nil
			i.pos = 0
			return nil, io.EOF
		}

		row := i.secondaryRows.Get()[i.pos]
		i.pos++
		return row, nil
	}

	if i.secondary == nil {
		var iter sql.RowIter
		iter, err = i.secondaryProvider.RowIter(ctx, i.primaryRow)
		if err != nil {
			return nil, err
		}

		i.secondary = iter
	}

	rightRow, err := i.secondary.Next(ctx)
	if err != nil {
		if err == io.EOF {
			err = i.secondary.Close(ctx)
			i.secondary = nil
			if err != nil {
				return nil, err
			}
			i.primaryRow = nil

			// If we got to this point and the mode is still unknown it means
			// the right side fits in memory, so the mode changes to memory
			// join.
			if i.mode == unknownMode {
				i.mode = memoryMode
			}

			return nil, io.EOF
		}
		return nil, err
	}

	if i.mode == unknownMode {
		var switchToMultipass bool
		if !ctx.Memory.HasAvailable() {
			switchToMultipass = true
		} else {
			err := i.secondaryRows.Add(rightRow)
			if err != nil && !sql.ErrNoMemoryAvailable.Is(err) {
				return nil, err
			}
		}

		if switchToMultipass {
			i.Dispose()
			i.secondaryRows = nil
			i.mode = multipassMode
		}
	}

	return rightRow, nil
}

func (i *joinIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if err := i.loadPrimary(ctx); err != nil {
			return nil, err
		}

		primary := i.primaryRow
		secondary, err := i.loadSecondary(ctx)
		if err != nil {
			if err == io.EOF {
				if !i.foundMatch && (i.typ == LeftJoinType || i.typ == RightJoinType) {
					row := i.buildRow(primary, nil)
					return row, nil
				}
				continue
			}
			return nil, err
		}

		row := i.buildRow(primary, secondary)
		matches, err := conditionIsTrue(ctx, row, i.cond)
		if err != nil {
			return nil, err
		}

		if !matches {
			continue
		}

		i.foundMatch = true
		return row, nil
	}
}

// buildRow builds the resulting row using the rows from the primary and
// secondary branches depending on the join type.
func (i *joinIter) buildRow(primary, secondary sql.Row) sql.Row {
	toCut := len(i.originalRow) - i.scopeLen
	row := make(sql.Row, i.rowSize-toCut)

	scope := primary[:i.scopeLen]
	primary = primary[len(i.originalRow):]

	var first, second sql.Row
	var secondOffset int
	switch i.typ {
	case RightJoinType:
		first = secondary
		second = primary
		secondOffset = len(row) - len(second)
	default:
		first = primary
		second = secondary
		secondOffset = i.scopeLen + len(first)
	}

	copy(row, scope)
	copy(row[i.scopeLen:], first)
	copy(row[secondOffset:], second)

	return row
}

func (i *joinIter) Close(ctx *sql.Context) (err error) {
	i.Dispose()

	if i.primary != nil {
		if err = i.primary.Close(ctx); err != nil {
			if i.secondary != nil {
				_ = i.secondary.Close(ctx)
			}
			return err
		}

	}

	if i.secondary != nil {
		err = i.secondary.Close(ctx)
		i.secondary = nil
	}

	return err
}

type existsIter struct {
	typ               JoinType
	primary           sql.RowIter
	secondaryProvider rowIterProvider
	secondary         sql.RowIter
	cond              sql.Expression

	primaryRow sql.Row

	// scope variables from outer scope
	originalRow sql.Row
	scopeLen    int
	rowSize     int
	dispose     sql.DisposeFunc
}

func (i *existsIter) Dispose() {
	if i.dispose != nil {
		i.dispose()
		i.dispose = nil
	}
}

func (i *existsIter) loadPrimary(ctx *sql.Context) error {
	if i.primaryRow == nil {
		r, err := i.primary.Next(ctx)
		if err != nil {
			return err
		}

		i.primaryRow = i.originalRow.Append(r)
	}

	return nil
}

func (i *existsIter) loadSecondary(ctx *sql.Context, left sql.Row) (row sql.Row, err error) {
	iter, err := i.secondaryProvider.RowIter(ctx, left)
	return iter.Next(ctx)
}

func (i *existsIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		r, err := i.primary.Next(ctx)
		if err != nil {
			return nil, err
		}
		left := i.originalRow.Append(r)
		rIter, err := i.secondaryProvider.RowIter(ctx, left)
		defer rIter.Close(ctx)
		if err != nil {
			return nil, err
		}
		for {
			right, err := rIter.Next(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					if i.typ == SemiJoinType {
						// reset iter, no match
						break
					}
					return left, nil
				}
				return nil, err
			}

			row := i.buildRow(left, right)
			matches, err := conditionIsTrue(ctx, row, i.cond)
			if !matches {
				continue
			}
			if i.typ == AntiJoinType {
				// reset iter, found match -> no return row
				break
			}
			return left, nil
		}
	}
	i.primary.Close(ctx)
	return nil, io.EOF

}

func (i *existsIter) buildRow(primary, secondary sql.Row) sql.Row {
	toCut := len(i.originalRow) - i.scopeLen
	row := make(sql.Row, i.rowSize-toCut)

	scope := primary[:i.scopeLen]
	primary = primary[len(i.originalRow):]

	var first, second sql.Row
	var secondOffset int
	switch i.typ {
	case RightJoinType:
		first = secondary
		second = primary
		secondOffset = len(row) - len(second)
	default:
		first = primary
		second = secondary
		secondOffset = i.scopeLen + len(first)
	}

	copy(row, scope)
	copy(row[i.scopeLen:], first)
	copy(row[secondOffset:], second)

	return row
}

func (i *existsIter) Close(ctx *sql.Context) (err error) {
	if i.primary != nil {
		if err = i.primary.Close(ctx); err != nil {
			if i.secondary != nil {
				_ = i.secondary.Close(ctx)
			}
			return err
		}

	}

	if i.secondary != nil {
		err = i.secondary.Close(ctx)
		i.secondary = nil
	}

	return err
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

func nodeHasJoin(node sql.Node) bool {
	hasJoinNode := false
	transform.Inspect(node, func(node sql.Node) bool {
		switch node.(type) {
		case JoinNode, *CrossJoin, *IndexedJoin:
			hasJoinNode = true
			return false
		default:
			return true
		}
	})

	return hasJoinNode
}

func isRightOrLeftJoin(node sql.Node) bool {
	jn, ok := node.(JoinNode)
	if !ok {
		return false
	}

	return jn.JoinType() == LeftJoinType || jn.JoinType() == RightJoinType
}

var _ sql.Node = (*FullOuterJoin)(nil)
var _ JoinNode = (*FullOuterJoin)(nil)
var _ sql.Expressioner = (*FullOuterJoin)(nil)

func NewFullOuterJoin(left, right sql.Node, filter sql.Expression) *FullOuterJoin {
	return &FullOuterJoin{
		&joinBase{
			Op:         FullOuterJoinType,
			BinaryNode: BinaryNode{left: left, right: right},
			Filter:     filter,
		},
	}
}

// FullOuterJoin returns all rows from two tables, deduplicating
// rows that match a join condition. This is equivalent to
// FULL_JOIN(A,B) => UNION(LEFT_JOIN(A,B), RIGHT_JOIN(A,B)).
type FullOuterJoin struct {
	*joinBase
}

func (j *FullOuterJoin) Schema() sql.Schema {
	return append(makeNullable(j.left.Schema()), makeNullable(j.right.Schema())...)
}

func (j *FullOuterJoin) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	iter, err := joinRowIter(ctx, LeftJoinType, j.left, j.right, j.Filter, row, j.ScopeLen, j.JoinMode)
	if err != nil {
		return nil, err
	}
	iter = &unionIter{
		cur: iter,
		nextIter: func(ctx *sql.Context) (sql.RowIter, error) {
			return joinRowIter(ctx, RightJoinType, j.left, j.right, j.Filter, row, j.ScopeLen, j.JoinMode)
		},
	}
	return newDistinctIter(ctx, iter), nil
}

func NewSemiJoin(left, right sql.Node, filter sql.Expression) *SemiJoin {
	return &SemiJoin{
		&existsJoinBase{
			&joinBase{
				Op:         SemiJoinType,
				BinaryNode: BinaryNode{left: left, right: right},
				Filter:     filter,
			},
		},
	}
}

func NewAntiJoin(left, right sql.Node, filter sql.Expression) *AntiJoin {
	return &AntiJoin{
		&existsJoinBase{
			&joinBase{
				Op:         AntiJoinType,
				BinaryNode: BinaryNode{left: left, right: right},
				Filter:     filter,
			},
		},
	}
}

// SemiJoin represents a set intersect (WHERE EXISTS) between
// two tables on a filter condition. In other words, a semi join
// returns rows from the left table if one or more rows
// from the right table match the join condition.
type SemiJoin struct {
	*existsJoinBase
}

// AntiJoin represents a NOT set intersect (WHERE NOT EXISTS) between
// two tables on a filter condition. In other words, an anit join
// returns rows from the left table only if zero rows
// from the right table match the join condition.
type AntiJoin struct {
	*existsJoinBase
}

type existsJoinBase struct {
	*joinBase
}

var _ sql.Node = (*AntiJoin)(nil)
var _ JoinNode = (*AntiJoin)(nil)
var _ sql.Expressioner = (*AntiJoin)(nil)
var _ sql.Node = (*SemiJoin)(nil)
var _ JoinNode = (*SemiJoin)(nil)
var _ sql.Expressioner = (*SemiJoin)(nil)

func (j *existsJoinBase) Schema() sql.Schema {
	return j.left.Schema()
}

func (j *existsJoinBase) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	leftIter, err := j.left.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}
	return &existsIter{
		originalRow:       row,
		typ:               j.Op,
		primary:           leftIter,
		secondaryProvider: j.right,
		cond:              j.Filter,
		scopeLen:          j.ScopeLen,
		rowSize:           len(row) + len(j.left.Schema()) + len(j.right.Schema()),
	}, nil
}
