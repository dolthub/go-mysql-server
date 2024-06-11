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

package jsontests

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type prepareJsonValue = func(*testing.T, interface{}) interface{}

type jsonFormatTest struct {
	name        string
	prepareFunc prepareJsonValue
}

var jsonFormatTests = []jsonFormatTest{
	{
		name: "string",
		prepareFunc: func(t *testing.T, js interface{}) interface{} {
			jsonString, _, err := types.Text.Convert(js)
			require.NoError(t, err)
			return jsonString
		},
	},
	{
		name: "JsonDocument",
		prepareFunc: func(t *testing.T, js interface{}) interface{} {
			doc, _, err := types.JSON.Convert(js)
			require.NoError(t, err)
			val, err := doc.(sql.JSONWrapper).ToInterface()
			require.NoError(t, err)
			return types.JSONDocument{Val: val}
		},
	},
	{
		name: "LazyJsonDocument",
		prepareFunc: func(t *testing.T, js interface{}) interface{} {
			doc, _, err := types.JSON.Convert(js)
			require.NoError(t, err)
			bytes, err := types.MarshallJson(doc.(sql.JSONWrapper))
			require.NoError(t, err)
			return types.NewLazyJSONDocument(bytes)
		},
	},
}

type testCase struct {
	f        sql.Expression
	row      sql.Row
	expected interface{}
	err      error
	name     string
}

func buildGetFieldExpressions(t *testing.T, construct func(...sql.Expression) (sql.Expression, error), argCount int) sql.Expression {
	expressions := make([]sql.Expression, 0, argCount)
	for i := 0; i < argCount; i++ {
		expressions = append(expressions, expression.NewGetField(i, types.LongText, "arg"+strconv.Itoa(i), false))
	}

	result, err := construct(expressions...)
	require.NoError(t, err)

	return result
}

func RunJsonTests(t *testing.T, testCases []testCase) {
	for _, tstC := range testCases {
		var paths []string
		for _, path := range tstC.row[1:] {
			if _, ok := path.(string); ok {
				paths = append(paths, path.(string))
			}
		}

		t.Run(tstC.name+"."+tstC.f.String()+"."+strings.Join(paths, ","), func(t *testing.T) {
			req := require.New(t)
			result, err := tstC.f.Eval(sql.NewEmptyContext(), tstC.row)
			if tstC.err == nil {
				req.NoError(err)

				var expect interface{}
				if tstC.expected != nil {
					expect, _, err = types.JSON.Convert(tstC.expected)
					if err != nil {
						panic("Bad test string. Can't convert string to JSONDocument: " + tstC.expected.(string))
					}
				}

				cmp, err := types.JSON.Compare(expect, result)
				req.NoError(err)
				if cmp != 0 {
					t.Error("Not equal:")
					t.Errorf("expected: %v", expect)
					t.Errorf("actual: %v", result)
					t.Fail()
				}
			} else {
				req.Error(err, "Expected an error but got %v", result)
				req.Contains(err.Error(), tstC.err.Error())
			}
		})
	}
}
