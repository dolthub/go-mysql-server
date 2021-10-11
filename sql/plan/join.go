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
	"io"
	"os"
	"reflect"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"

	"github.com/dolthub/go-mysql-server/sql"
)

const (
	inMemoryJoinKey        = "INMEMORY_JOINS"
	inMemoryJoinSessionVar = "inmemory_joins"
)

var useInMemoryJoins = shouldUseMemoryJoinsByEnv()

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
type joinStruct struct {
	BinaryNode
	Cond       sql.Expression
	CommentStr string
	ScopeLen   int
	JoinMode   joinMode
}

// Expressions implements sql.Expression
func (j joinStruct) Expressions() []sql.Expression {
	return []sql.Expression{j.Cond}
}

func (j joinStruct) JoinCond() sql.Expression {
	return j.Cond
}

// Comment implements sql.CommentedNode
func (j joinStruct) Comment() string {
	return j.CommentStr
}

// InnerJoin is an inner join between two tables.
type InnerJoin struct {
	joinStruct
}

var _ JoinNode = (*InnerJoin)(nil)
var _ sql.CommentedNode = (*InnerJoin)(nil)

func (j *InnerJoin) JoinType() JoinType {
	return JoinTypeInner
}

// NewInnerJoin creates a new inner join node from two tables.
func NewInnerJoin(left, right sql.Node, cond sql.Expression) *InnerJoin {
	return &InnerJoin{
		joinStruct{
			BinaryNode: BinaryNode{
				left:  left,
				right: right,
			},
			Cond: cond,
		},
	}
}

// Schema implements the Node interface.
func (j *InnerJoin) Schema() sql.Schema {
	return append(j.left.Schema(), j.right.Schema()...)
}

// Resolved implements the Resolvable interface.
func (j *InnerJoin) Resolved() bool {
	return j.left.Resolved() && j.right.Resolved() && j.Cond.Resolved()
}

// RowIter implements the Node interface.
func (j *InnerJoin) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return joinRowIter(ctx, JoinTypeInner, j.left, j.right, j.Cond, row, j.ScopeLen, j.JoinMode)
}

// WithChildren implements the Node interface.
func (j *InnerJoin) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(children), 2)
	}

	nj := *j
	nj.BinaryNode = BinaryNode{children[0], children[1]}
	return &nj, nil
}

func (j *InnerJoin) WithScopeLen(i int) JoinNode {
	nj := *j
	nj.ScopeLen = i
	return &nj
}

func (j InnerJoin) WithMultipassMode() JoinNode {
	j.JoinMode = multipassMode
	return &j
}

// WithExpressions implements the Expressioner interface.
func (j *InnerJoin) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(exprs), 1)
	}

	nj := *j
	nj.Cond = exprs[0]
	return &nj, nil
}

// WithComment implements sql.CommentedNode
func (j *InnerJoin) WithComment(comment string) sql.Node {
	nj := *j
	nj.CommentStr = comment
	return &nj
}

func (j *InnerJoin) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("InnerJoin%s", j.Cond)
	_ = pr.WriteChildren(j.left.String(), j.right.String())
	return pr.String()
}

func (j *InnerJoin) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("InnerJoin%s, comment=%s", sql.DebugString(j.Cond), j.Comment())
	_ = pr.WriteChildren(sql.DebugString(j.left), sql.DebugString(j.right))
	return pr.String()
}

// LeftJoin is a left join between two tables.
type LeftJoin struct {
	joinStruct
}

var _ JoinNode = (*LeftJoin)(nil)
var _ sql.CommentedNode = (*LeftJoin)(nil)

func (j *LeftJoin) JoinType() JoinType {
	return JoinTypeLeft
}

// NewLeftJoin creates a new left join node from two tables.
func NewLeftJoin(left, right sql.Node, cond sql.Expression) *LeftJoin {
	return &LeftJoin{
		joinStruct{
			BinaryNode: BinaryNode{
				left:  left,
				right: right,
			},
			Cond: cond,
		},
	}
}

// Schema implements the Node interface.
func (j *LeftJoin) Schema() sql.Schema {
	return append(j.left.Schema(), makeNullable(j.right.Schema())...)
}

// Resolved implements the Resolvable interface.
func (j *LeftJoin) Resolved() bool {
	return j.left.Resolved() && j.right.Resolved() && j.Cond.Resolved()
}

// RowIter implements the Node interface.
func (j *LeftJoin) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return joinRowIter(ctx, JoinTypeLeft, j.left, j.right, j.Cond, row, j.ScopeLen, j.JoinMode)
}

// WithChildren implements the Node interface.
func (j *LeftJoin) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(children), 1)
	}

	nj := *j
	nj.BinaryNode = BinaryNode{children[0], children[1]}
	return &nj, nil
}

// WithExpressions implements the Expressioner interface.
func (j *LeftJoin) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(exprs), 1)
	}

	nj := *j
	nj.Cond = exprs[0]
	return &nj, nil
}

func (j *LeftJoin) WithScopeLen(i int) JoinNode {
	nj := *j
	nj.ScopeLen = i
	return &nj
}

func (j LeftJoin) WithMultipassMode() JoinNode {
	j.JoinMode = multipassMode
	return &j
}

// WithComment implements sql.CommentedNode
func (j *LeftJoin) WithComment(comment string) sql.Node {
	nj := *j
	nj.CommentStr = comment
	return &nj
}

func (j *LeftJoin) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("LeftJoin%s", j.Cond)
	_ = pr.WriteChildren(j.left.String(), j.right.String())
	return pr.String()
}

func (j *LeftJoin) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("LeftJoin%s", sql.DebugString(j.Cond))
	_ = pr.WriteChildren(sql.DebugString(j.left), sql.DebugString(j.right))
	return pr.String()
}

// RightJoin is a left join between two tables.
type RightJoin struct {
	joinStruct
}

func (j *RightJoin) JoinType() JoinType {
	return JoinTypeRight
}

var _ JoinNode = (*RightJoin)(nil)
var _ sql.CommentedNode = (*RightJoin)(nil)

// NewRightJoin creates a new right join node from two tables.
func NewRightJoin(left, right sql.Node, cond sql.Expression) *RightJoin {
	return &RightJoin{
		joinStruct{
			BinaryNode: BinaryNode{
				left:  left,
				right: right,
			},
			Cond: cond,
		},
	}
}

// Schema implements the Node interface.
func (j *RightJoin) Schema() sql.Schema {
	return append(makeNullable(j.left.Schema()), j.right.Schema()...)
}

// Resolved implements the Resolvable interface.
func (j *RightJoin) Resolved() bool {
	return j.left.Resolved() && j.right.Resolved() && j.Cond.Resolved()
}

// RowIter implements the Node interface.
func (j *RightJoin) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return joinRowIter(ctx, JoinTypeRight, j.left, j.right, j.Cond, row, j.ScopeLen, j.JoinMode)
}

// WithChildren implements the Node interface.
func (j *RightJoin) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(children), 2)
	}

	nj := *j
	nj.BinaryNode = BinaryNode{children[0], children[1]}
	return &nj, nil
}

// WithExpressions implements the Expressioner interface.
func (j *RightJoin) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(exprs), 1)
	}

	nj := *j
	nj.Cond = exprs[0]
	return &nj, nil
}

func (j *RightJoin) WithScopeLen(i int) JoinNode {
	nj := *j
	nj.ScopeLen = i
	return &nj
}

func (j RightJoin) WithMultipassMode() JoinNode {
	j.JoinMode = multipassMode
	return &j
}

// WithComment implements sql.CommentedNode
func (j *RightJoin) WithComment(comment string) sql.Node {
	nj := *j
	nj.CommentStr = comment
	return &nj
}

func (j *RightJoin) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("RightJoin%s", j.Cond)
	_ = pr.WriteChildren(j.left.String(), j.right.String())
	return pr.String()
}

func (j *RightJoin) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("RightJoin%s", sql.DebugString(j.Cond))
	_ = pr.WriteChildren(sql.DebugString(j.left), sql.DebugString(j.right))
	return pr.String()
}

type JoinType byte

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeRight
)

func (t JoinType) String() string {
	switch t {
	case JoinTypeInner:
		return "InnerJoin"
	case JoinTypeLeft:
		return "LeftJoin"
	case JoinTypeRight:
		return "RightJoin"
	default:
		return "INVALID"
	}
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

	span, ctx := ctx.Span("plan."+typ.String(), opentracing.Tags{
		"left":  leftName,
		"right": rightName,
	})

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
	if typ == JoinTypeRight {
		r, err := right.RowIter(ctx, row)
		if err != nil {
			span.Finish()
			return nil, err
		}
		return sql.NewSpanIter(span, &joinIter{
			typ:               typ,
			primary:           r,
			secondaryProvider: left,
			ctx:               ctx,
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
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, &joinIter{
		typ:               typ,
		primary:           l,
		secondaryProvider: right,
		ctx:               ctx,
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
	ctx               *sql.Context
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

func (i *joinIter) loadPrimary() error {
	if i.primaryRow == nil {
		r, err := i.primary.Next()
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

func (i *joinIter) loadSecondaryInMemory() error {
	iter, err := i.secondaryProvider.RowIter(i.ctx, i.primaryRow)
	if err != nil {
		return err
	}

	for {
		row, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := i.secondaryRows.Add(row); err != nil {
			return err
		}
	}

	if len(i.secondaryRows.Get()) == 0 {
		return io.EOF
	}

	return nil
}

func (i *joinIter) loadSecondary() (row sql.Row, err error) {
	if i.mode == memoryMode {
		if len(i.secondaryRows.Get()) == 0 {
			if err = i.loadSecondaryInMemory(); err != nil {
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
		iter, err = i.secondaryProvider.RowIter(i.ctx, i.primaryRow)
		if err != nil {
			return nil, err
		}

		i.secondary = iter
	}

	rightRow, err := i.secondary.Next()
	if err != nil {
		if err == io.EOF {
			i.secondary = nil
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
		if !i.ctx.Memory.HasAvailable() {
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

func (i *joinIter) Next() (sql.Row, error) {
	for {
		if err := i.loadPrimary(); err != nil {
			return nil, err
		}

		primary := i.primaryRow
		secondary, err := i.loadSecondary()
		if err != nil {
			if err == io.EOF {
				if !i.foundMatch && (i.typ == JoinTypeLeft || i.typ == JoinTypeRight) {
					row := i.buildRow(primary, nil)
					return row, nil
				}
				continue
			}
			return nil, err
		}

		row := i.buildRow(primary, secondary)
		matches, err := conditionIsTrue(i.ctx, row, i.cond)
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
	case JoinTypeRight:
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
	i.secondary = nil

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
