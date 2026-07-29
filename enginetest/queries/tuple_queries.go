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

package queries

import (
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
)

type tupleEqualityTest struct {
	left, right          []interface{}
	isEqual              interface{}
	isGreater            interface{}
	isGreaterIfReordered interface{}
	skipSubqueryTests    bool
}

var tupleEqualityTests = []tupleEqualityTest{
	{
		left:                 []interface{}{1, 2},
		right:                []interface{}{nil, 2},
		isEqual:              nil,
		isGreater:            nil,
		isGreaterIfReordered: nil,
		skipSubqueryTests:    true,
	},
	{
		left:                 []interface{}{1, 2},
		right:                []interface{}{nil, 3},
		isEqual:              false,
		isGreater:            nil,
		isGreaterIfReordered: false,
		skipSubqueryTests:    true,
	},
	{
		left:                 []interface{}{0, nil},
		right:                []interface{}{0, nil},
		isEqual:              nil,
		isGreater:            nil,
		isGreaterIfReordered: nil,
	},
}

func mustBuildBindVariable(v interface{}) sqlparser.Expr {
	bv, err := sqltypes.BuildBindVariable(v)
	if err != nil {
		panic(err)
	}
	val, err := sqltypes.BindVariableToValue(bv)
	if err != nil {
		panic(err)
	}
	ret, err := sqlparser.ExprFromValue(val)
	if err != nil {
		panic(err)
	}
	return ret
}

func not(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	return !v.(bool)
}

func MakeTupleQueryTests(cb func(test QueryTest)) {
	testEquality := func(testExpression string, left, right []interface{}, expected interface{}) {
		makeTests := func(queryString string, expectedRows []sql.Row) {
			cb(QueryTest{
				Query:    queryString,
				Expected: expectedRows,
				Bindings: map[string]sqlparser.Expr{
					"v1": mustBuildBindVariable(left[0]),
					"v2": mustBuildBindVariable(left[1]),
					"v3": mustBuildBindVariable(right[0]),
					"v4": mustBuildBindVariable(right[1]),
				},
			})
			cb(QueryTest{
				Query:    queryString,
				Expected: expectedRows,
				Bindings: map[string]sqlparser.Expr{
					"v1": mustBuildBindVariable(right[0]),
					"v2": mustBuildBindVariable(right[1]),
					"v3": mustBuildBindVariable(left[0]),
					"v4": mustBuildBindVariable(left[1]),
				},
			})
			cb(QueryTest{
				Query:    queryString,
				Expected: expectedRows,
				Bindings: map[string]sqlparser.Expr{
					"v1": mustBuildBindVariable(left[1]),
					"v2": mustBuildBindVariable(left[0]),
					"v3": mustBuildBindVariable(right[1]),
					"v4": mustBuildBindVariable(right[0]),
				},
			})
			cb(QueryTest{
				Query:    queryString,
				Expected: expectedRows,
				Bindings: map[string]sqlparser.Expr{
					"v1": mustBuildBindVariable(right[1]),
					"v2": mustBuildBindVariable(right[0]),
					"v3": mustBuildBindVariable(left[1]),
					"v4": mustBuildBindVariable(left[0]),
				},
			})
		}
		makeTests("SELECT "+testExpression, []sql.Row{{expected}})
		var filteredRows []sql.Row
		if expected == true {
			filteredRows = []sql.Row{{1}}
		} else {
			filteredRows = []sql.Row{}
		}
		makeTests("SELECT 1 WHERE "+testExpression, filteredRows)
	}

	testInquality := func(left0, left1, right0, right1 interface{}, expected interface{}) {
		cb(QueryTest{
			Query:    "SELECT (?, ?) > (?, ?)",
			Expected: []sql.Row{{expected}},
			Bindings: map[string]sqlparser.Expr{
				"v1": mustBuildBindVariable(left0),
				"v2": mustBuildBindVariable(left1),
				"v3": mustBuildBindVariable(right0),
				"v4": mustBuildBindVariable(right1),
			},
		})
		cb(QueryTest{
			Query:    "SELECT (?, ?) <= (?, ?)",
			Expected: []sql.Row{{not(expected)}},
			Bindings: map[string]sqlparser.Expr{
				"v1": mustBuildBindVariable(left0),
				"v2": mustBuildBindVariable(left1),
				"v3": mustBuildBindVariable(right0),
				"v4": mustBuildBindVariable(right1),
			},
		})
		cb(QueryTest{
			Query:    "SELECT (?, ?) < (?, ?)",
			Expected: []sql.Row{{expected}},
			Bindings: map[string]sqlparser.Expr{
				"v1": mustBuildBindVariable(right0),
				"v2": mustBuildBindVariable(right1),
				"v3": mustBuildBindVariable(left0),
				"v4": mustBuildBindVariable(left1),
			},
		})
		cb(QueryTest{
			Query:    "SELECT (?, ?) >= (?, ?)",
			Expected: []sql.Row{{not(expected)}},
			Bindings: map[string]sqlparser.Expr{
				"v1": mustBuildBindVariable(right0),
				"v2": mustBuildBindVariable(right1),
				"v3": mustBuildBindVariable(left0),
				"v4": mustBuildBindVariable(left1),
			},
		})
	}

	for _, test := range tupleEqualityTests {
		testEquality("(?, ?) = (?, ?)", test.left, test.right, test.isEqual)
		testEquality("(?, ?) IN ((?, ?))", test.left, test.right, test.isEqual)
		testEquality("(?, ?) != (?, ?)", test.left, test.right, not(test.isEqual))
		testEquality("(?, ?) NOT IN ((?, ?))", test.left, test.right, not(test.isEqual))
		if !test.skipSubqueryTests {
			testEquality("(?, ?) IN (SELECT ?, ?)", test.left, test.right, test.isEqual)
			testEquality("(?, ?) NOT IN (SELECT ?, ?)", test.left, test.right, test.isEqual)
		}
		testInquality(test.left[0], test.left[1], test.right[0], test.right[1], test.isGreater)
		testInquality(test.left[1], test.left[0], test.right[1], test.right[0], test.isGreaterIfReordered)
	}
}
