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

package function

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type FuncTest struct {
	name      string
	expr      sql.Expression
	expected  interface{}
	expectErr bool
}

func (ft FuncTest) Run(t *testing.T, ctx *sql.Context, r sql.Row) {
	t.Run(ft.name, func(t *testing.T) {
		expr, err := ft.expr.WithChildren(ctx, ft.expr.Children()...)
		require.NoError(t, err)

		res, err := expr.Eval(ctx, r)

		if ft.expectErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, ft.expected, res)
			assertResultType(t, ft.expr.Type(), res)
		}
	})
}

func assertResultType(t *testing.T, expectedType sql.Type, result interface{}) {
	if result == nil {
		return
	}

	switch result.(type) {
	case uint8:
		assert.Equal(t, expectedType, sql.Uint8)
	case int8:
		assert.Equal(t, expectedType, sql.Int8)
	case uint16:
		assert.Equal(t, expectedType, sql.Uint16)
	case int16:
		assert.Equal(t, expectedType, sql.Int16)
	case uint32, uint:
		assert.Equal(t, expectedType, sql.Uint32)
	case int32, int:
		assert.Equal(t, expectedType, sql.Int32)
	case uint64:
		assert.Equal(t, expectedType, sql.Uint64)
	case int64:
		assert.Equal(t, expectedType, sql.Int64)
	case float64:
		assert.Equal(t, expectedType, sql.Float64)
	case float32:
		assert.Equal(t, expectedType, sql.Float32)
	case string:
		assert.True(t, sql.IsText(expectedType))
	case time.Time:
		assert.Equal(t, expectedType, sql.Datetime)
	case bool:
		assert.Equal(t, expectedType, sql.Boolean)
	default:
		assert.Fail(t, "unhandled case")
	}
}

type TestFactory struct {
	createFunc interface{}
	Tests      []FuncTest
}

func NewTestFactory(createFunc interface{}) *TestFactory {
	switch createFunc.(type) {
	case sql.CreateFunc0Args, sql.CreateFunc1Args, sql.CreateFunc2Args, sql.CreateFunc3Args, sql.CreateFunc4Args, sql.CreateFunc5Args, sql.CreateFunc6Args, sql.CreateFunc7Args, sql.CreateFuncNArgs:
	default:
		panic("Unsupported create method")
	}

	return &TestFactory{createFunc: createFunc}
}

func (tf *TestFactory) Test(t *testing.T, ctx *sql.Context, r sql.Row) {
	for _, test := range tf.Tests {
		test.Run(t, ctx, r)
	}
}

func (tf *TestFactory) AddTest(name string, expected interface{}, expectErr bool, inputs ...interface{}) {
	var test FuncTest

	ctx := sql.NewEmptyContext()
	inputExprs := toLiteralExpressions(inputs)
	switch fn := tf.createFunc.(type) {
	case sql.CreateFunc0Args:
		if len(inputExprs) != 0 {
			panic(fmt.Sprintf("error in test: %s. params provided: %d, params required: %d", name, len(inputExprs), 0))
		}
		sqlFuncExpr := fn(ctx)
		test = FuncTest{name, sqlFuncExpr, expected, expectErr}

	case sql.CreateFunc1Args:
		if len(inputExprs) != 1 {
			panic(fmt.Sprintf("error in test: %s. params provided: %d, params required: %d", name, len(inputExprs), 1))
		}
		sqlFuncExpr := fn(ctx, inputExprs[0])
		test = FuncTest{name, sqlFuncExpr, expected, expectErr}

	case sql.CreateFunc2Args:
		if len(inputExprs) != 2 {
			panic(fmt.Sprintf("error in test: %s. params provided: %d, params required: %d", name, len(inputExprs), 2))
		}
		sqlFuncExpr := fn(ctx, inputExprs[0], inputExprs[1])
		test = FuncTest{name, sqlFuncExpr, expected, expectErr}

	case sql.CreateFunc3Args:
		if len(inputExprs) != 3 {
			panic(fmt.Sprintf("error in test: %s. params provided: %d, params required: %d", name, len(inputExprs), 3))
		}
		sqlFuncExpr := fn(ctx, inputExprs[0], inputExprs[1], inputExprs[2])
		test = FuncTest{name, sqlFuncExpr, expected, expectErr}

	case sql.CreateFunc4Args:
		if len(inputExprs) != 4 {
			panic(fmt.Sprintf("error in test: %s. params provided: %d, params required: %d", name, len(inputExprs), 4))
		}
		sqlFuncExpr := fn(ctx, inputExprs[0], inputExprs[1], inputExprs[2], inputExprs[3])
		test = FuncTest{name, sqlFuncExpr, expected, expectErr}

	case sql.CreateFunc5Args:
		if len(inputExprs) != 5 {
			panic(fmt.Sprintf("error in test: %s. params provided: %d, params required: %d", name, len(inputExprs), 5))
		}
		sqlFuncExpr := fn(ctx, inputExprs[0], inputExprs[1], inputExprs[2], inputExprs[3], inputExprs[4])
		test = FuncTest{name, sqlFuncExpr, expected, expectErr}

	case sql.CreateFunc6Args:
		if len(inputExprs) != 6 {
			panic(fmt.Sprintf("error in test: %s. params provided: %d, params required: %d", name, len(inputExprs), 6))
		}
		sqlFuncExpr := fn(ctx, inputExprs[0], inputExprs[1], inputExprs[2], inputExprs[3], inputExprs[4], inputExprs[5])
		test = FuncTest{name, sqlFuncExpr, expected, expectErr}

	case sql.CreateFunc7Args:
		if len(inputExprs) != 7 {
			panic(fmt.Sprintf("error in test: %s. params provided: %d, params required: %d", name, len(inputExprs), 7))
		}
		sqlFuncExpr := fn(ctx, inputExprs[0], inputExprs[1], inputExprs[2], inputExprs[3], inputExprs[4], inputExprs[5], inputExprs[6])
		test = FuncTest{name, sqlFuncExpr, expected, expectErr}

	case sql.CreateFuncNArgs:
		sqlFuncExpr, err := fn(ctx, inputExprs...)

		if err != nil {
			panic(fmt.Sprintf("error in test: %s. %s", name, err.Error()))
		}

		test = FuncTest{name, sqlFuncExpr, expected, expectErr}

	default:
		panic("should never get here.  Should have failed in NewTestFactory already.")
	}

	tf.Tests = append(tf.Tests, test)
}

func (tf *TestFactory) AddSucceeding(expected interface{}, args ...interface{}) {
	name := generateNameFromArgs(args...)
	tf.AddTest(name, expected, false, args...)
}

func (tf *TestFactory) AddFailing(args ...interface{}) {
	name := generateNameFromArgs(args...)
	tf.AddTest(name, nil, true, args...)
}

func isValidIntOfSize(bits int, n int64) bool {
	var max int64 = 1<<(bits-1) - 1
	var min int64 = -(1 << (bits - 1))
	return n < max && n > min
}

func (tf *TestFactory) AddSignedVariations(expected interface{}, arg int64) {
	if isValidIntOfSize(8, arg) {
		tf.AddSucceeding(expected, int8(arg))
	}

	if isValidIntOfSize(16, arg) {
		tf.AddSucceeding(expected, int16(arg))
	}

	if isValidIntOfSize(32, arg) {
		tf.AddSucceeding(expected, int32(arg))
		tf.AddSucceeding(expected, int(arg))
	}

	tf.AddSucceeding(expected, arg)
}

func isValidUintOfSize(bits int, n uint64) bool {
	var max uint64 = 1<<bits - 1
	return n < max
}

func (tf *TestFactory) AddUnsignedVariations(expected interface{}, arg uint64) {
	if isValidUintOfSize(8, arg) {
		tf.AddSucceeding(expected, uint8(arg))
	}

	if isValidUintOfSize(16, arg) {
		tf.AddSucceeding(expected, uint16(arg))
	}

	if isValidUintOfSize(32, arg) {
		tf.AddSucceeding(expected, uint32(arg))
		tf.AddSucceeding(expected, uint(arg))
	}

	tf.AddSucceeding(expected, arg)
}

func (tf *TestFactory) AddFloatVariations(expected interface{}, f float64) {
	tf.AddSucceeding(expected, f)
	tf.AddSucceeding(expected, float32(f))
	tf.AddSucceeding(expected, decimal.NewFromFloat(f))
}

func generateNameFromArgs(args ...interface{}) string {
	name := "("

	for i, arg := range args {
		if i > 0 {
			name += ","
		}

		if arg == nil {
			name += "nil"
		} else {
			name += fmt.Sprintf("%s{%v}", reflect.TypeOf(arg).String(), arg)
		}
	}

	name += ")"

	return name
}

func toLiteralExpressions(inputs []interface{}) []sql.Expression {
	literals := make([]sql.Expression, len(inputs))
	for i, in := range inputs {
		literals[i] = toLiteralExpression(in)
	}

	return literals
}

func toLiteralExpression(input interface{}) *expression.Literal {
	if input == nil {
		return expression.NewLiteral(nil, sql.Null)
	}

	switch val := input.(type) {
	case bool:
		return expression.NewLiteral(val, sql.Boolean)
	case int8:
		return expression.NewLiteral(val, sql.Int8)
	case uint8:
		return expression.NewLiteral(val, sql.Int8)
	case int16:
		return expression.NewLiteral(val, sql.Int16)
	case uint16:
		return expression.NewLiteral(val, sql.Uint16)
	case int32:
		return expression.NewLiteral(val, sql.Int32)
	case uint32:
		return expression.NewLiteral(val, sql.Uint32)
	case int64:
		return expression.NewLiteral(val, sql.Int64)
	case uint64:
		return expression.NewLiteral(val, sql.Uint64)
	case int:
		return expression.NewLiteral(val, sql.Int32)
	case uint:
		return expression.NewLiteral(val, sql.Uint32)
	case float32:
		return expression.NewLiteral(val, sql.Float32)
	case float64:
		return expression.NewLiteral(val, sql.Float64)
	case decimal.Decimal:
		return expression.NewLiteral(val, sql.Float64)
	case string:
		return expression.NewLiteral(val, sql.Text)
	case time.Time:
		return expression.NewLiteral(val, sql.Datetime)
	case []byte:
		return expression.NewLiteral(string(val), sql.Blob)
	default:
		panic("unsupported type")
	}
}
