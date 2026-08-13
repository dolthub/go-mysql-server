// Copyright 2024 Dolthub, Inc.
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// fakeEqualityExpr simulates an integrator-defined equality expression that implements expression.Equality.
type fakeEqualityExpr struct {
	expression.BinaryExpressionStub
	representsEquality bool
}

var _ sql.Expression = (*fakeEqualityExpr)(nil)
var _ expression.Equality = (*fakeEqualityExpr)(nil)

func (f *fakeEqualityExpr) String() string {
	return f.LeftChild.String() + " == " + f.RightChild.String()
}

func (f *fakeEqualityExpr) Type(ctx *sql.Context) sql.Type {
	return types.Boolean
}

func (f *fakeEqualityExpr) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	left, err := f.LeftChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	right, err := f.RightChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	return left == right, nil
}

func (f *fakeEqualityExpr) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return &fakeEqualityExpr{expression.BinaryExpressionStub{LeftChild: children[0], RightChild: children[1]}, f.representsEquality}, nil
}

func (f *fakeEqualityExpr) RepresentsEquality() bool {
	return f.representsEquality
}

func (f *fakeEqualityExpr) SwapParameters(ctx *sql.Context) (expression.Equality, error) {
	return &fakeEqualityExpr{expression.BinaryExpressionStub{LeftChild: f.RightChild, RightChild: f.LeftChild}, f.representsEquality}, nil
}

func (f *fakeEqualityExpr) ToComparer(ctx *sql.Context) (expression.Comparer, error) {
	return expression.NewEquals(f.LeftChild, f.RightChild), nil
}

// TestExtractJoinColumnExprMatchesEqualityInterface tests that extractJoinColumnExpr recognizes equality
// predicates, correctly extracting both sides of a matching predicate while still rejecting one whose
// RepresentsEquality() returns false.
func TestExtractJoinColumnExprMatchesEqualityInterface(t *testing.T) {
	ctx := sql.NewEmptyContext()
	left := expression.NewGetField(0, types.Int64, "a", false)
	right := expression.NewGetField(1, types.Int64, "b", false)
	eq := &fakeEqualityExpr{expression.BinaryExpressionStub{LeftChild: left, RightChild: right}, true}

	leftCol, rightCol := extractJoinColumnExpr(ctx, eq)
	require.NotNil(t, leftCol)
	require.NotNil(t, rightCol)
	require.Equal(t, left, leftCol.col)
	require.Equal(t, right, leftCol.comparand)
	require.Equal(t, right, rightCol.col)
	require.Equal(t, left, rightCol.comparand)

	// RepresentsEquality() == false must still be rejected
	nonEq := &fakeEqualityExpr{expression.BinaryExpressionStub{LeftChild: left, RightChild: right}, false}
	leftCol, rightCol = extractJoinColumnExpr(ctx, nonEq)
	require.Nil(t, leftCol)
	require.Nil(t, rightCol)
}
