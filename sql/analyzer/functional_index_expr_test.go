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

package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// mockOperator stands in for an integrator's own operator representation, e.g. doltgresql's
// framework.Operator (a named byte type, not a bare string).
type mockOperator byte

const (
	mockOperatorAdd mockOperator = iota
	mockOperatorMul
)

// mockBinaryExpr is a minimal sql.Expression whose Operator() method returns a non-string type,
// simulating an integrator's polymorphic binary expression type: one Go struct encoding multiple
// operators, distinguished by a field, whose Operator()-equivalent method isn't `Operator() string`.
type mockBinaryExpr struct {
	op          mockOperator
	left, right sql.Expression
}

var _ sql.Expression = (*mockBinaryExpr)(nil)

func (m *mockBinaryExpr) Resolved() bool                                  { return true }
func (m *mockBinaryExpr) String() string                                  { return "mockBinaryExpr" }
func (m *mockBinaryExpr) Type(*sql.Context) sql.Type                      { return types.Int64 }
func (m *mockBinaryExpr) IsNullable(*sql.Context) bool                    { return false }
func (m *mockBinaryExpr) Eval(*sql.Context, sql.Row) (interface{}, error) { return nil, nil }
func (m *mockBinaryExpr) Children() []sql.Expression                      { return []sql.Expression{m.left, m.right} }
func (m *mockBinaryExpr) WithChildren(_ *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return &mockBinaryExpr{op: m.op, left: children[0], right: children[1]}, nil
}

// Operator mirrors the shape of expression.Arithmetic.Operator() / doltgresql's
// BinaryOperator.Operator(), but returns mockOperator rather than string.
func (m *mockBinaryExpr) Operator() mockOperator { return m.op }

// TestExpressionsEquivalent_NonStringOperator guards against a real bug found in doltgresql: a
// polymorphic binary expression type whose Operator()-equivalent method returns a non-string type
// (there, framework.Operator, a named byte) silently failed the `interface{ Operator() string }`
// type assertion this function used to rely on, so two expressions with identical children but
// different operators (e.g. c1+c2 and c1*c2) were incorrectly treated as equivalent -- causing a
// functional index query to match the wrong hidden generated column.
func TestExpressionsEquivalent_NonStringOperator(t *testing.T) {
	col := func(name string) sql.Expression {
		return expression.NewGetField(0, types.Int64, name, false)
	}

	add := &mockBinaryExpr{op: mockOperatorAdd, left: col("c1"), right: col("c2")}
	addAgain := &mockBinaryExpr{op: mockOperatorAdd, left: col("c1"), right: col("c2")}
	mul := &mockBinaryExpr{op: mockOperatorMul, left: col("c1"), right: col("c2")}

	require.True(t, expressionsEquivalent(add, addAgain),
		"same operator and same children should be equivalent")
	require.False(t, expressionsEquivalent(add, mul),
		"different operator with identical children must NOT be equivalent")
}
