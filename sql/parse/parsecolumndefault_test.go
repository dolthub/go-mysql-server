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

package parse

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestStringToColumnDefaultValue(t *testing.T) {
	tests := []struct {
		exprStr      string
		expectedExpr sql.Expression
	}{
		{
			"2",
			NewColumnDefaultValue(
				expression.NewLiteral(int8(2), types.Int8),
				nil,
				true,
				false,
				true,
			),
		},
		{
			"(2)",
			NewColumnDefaultValue(
				expression.NewLiteral(int8(2), types.Int8),
				nil,
				false,
				true,
				true,
			),
		},
		{
			"(RAND() + 5)",
			NewColumnDefaultValue(
				expression.NewArithmetic(
					expression.NewUnresolvedFunction("rand", false, nil),
					expression.NewLiteral(int8(5), types.Int8),
					"+",
				),
				nil,
				false,
				true,
				true,
			),
		},
		{
			"(GREATEST(RAND(), RAND()))",
			NewColumnDefaultValue(
				expression.NewUnresolvedFunction("greatest", false, nil,
					expression.NewUnresolvedFunction("rand", false, nil),
					expression.NewUnresolvedFunction("rand", false, nil),
				),
				nil,
				false,
				true,
				true,
			),
		},
	}

	for _, test := range tests {
		t.Run(test.exprStr, func(t *testing.T) {
			res, err := StringToColumnDefaultValue(sql.NewEmptyContext(), test.exprStr)
			if test.expectedExpr == nil {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedExpr, res)
			}
		})
	}
}

// must executes functions of the form "func(args...) (sql.Expression, error)" and panics on errors
func must(f interface{}, args ...interface{}) sql.Expression {
	fType := reflect.TypeOf(f)
	if fType.Kind() != reflect.Func ||
		fType.NumOut() != 2 ||
		!fType.Out(0).AssignableTo(reflect.TypeOf((*sql.Expression)(nil)).Elem()) ||
		!fType.Out(1).AssignableTo(reflect.TypeOf((*error)(nil)).Elem()) {
		panic("invalid function given")
	}
	// we let reflection ensure that the arguments match
	argVals := make([]reflect.Value, len(args))
	for i, arg := range args {
		argVals[i] = reflect.ValueOf(arg)
	}
	fVal := reflect.ValueOf(f)
	out := fVal.Call(argVals)
	err, _ := out[1].Interface().(error)
	if err != nil {
		panic("must err is nil")
	}
	return out[0].Interface().(sql.Expression)
}

func NewColumnDefaultValue(expr sql.Expression, outType sql.Type, isLiteral, isParenthesized, mayReturnNil bool) *sql.ColumnDefaultValue {
	cdv, err := sql.NewColumnDefaultValue(expr, outType, isLiteral, isParenthesized, mayReturnNil)
	if err != nil {
		panic(err)
	}
	return cdv
}
