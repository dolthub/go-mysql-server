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
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func JsonContainsPathTestCases(t *testing.T, prepare prepareJsonValue) []testCase {

	// setup call expressions for calling with 1, 2, and 3 paths.
	onePath, err := json.NewJSONContainsPath(
		expression.NewGetField(0, types.JSON, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
	)
	require.NoError(t, err)

	twoPath, err := json.NewJSONContainsPath(
		expression.NewGetField(0, types.JSON, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
		expression.NewGetField(3, types.LongText, "arg4", false),
	)
	require.NoError(t, err)

	threePath, err := json.NewJSONContainsPath(
		expression.NewGetField(0, types.JSON, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
		expression.NewGetField(3, types.LongText, "arg4", false),
		expression.NewGetField(4, types.LongText, "arg5", false),
	)
	require.NoError(t, err)

	return []testCase{
		{f: onePath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1, "b": 2, "c": {"d": 4}}`), `oNe`, `$.a`}, expected: true},
		{f: onePath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1, "b": 2, "c": {"d": 4}}`), `one`, `$.e`}, expected: false},
		{f: onePath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1, "b": 2, "c": {"d": 4}}`), `all`, `$.e`}, expected: false},
		{f: onePath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1, "b": 2, "c": {"d": 4}}`), `All`, `$.c.d`}, expected: true},

		{f: onePath, row: sql.UntypedSqlRow{prepare(t, `[]`), `one`, `$[1]`}, expected: false},

		{f: twoPath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1, "b": 2, "c": {"d": 4}}`), `one`, `$.a`, `$.e`}, expected: true},
		{f: twoPath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1, "b": 2, "c": {"d": 4}}`), `ALL`, `$.a`, `$.e`}, expected: false},

		{f: twoPath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1, "b": 2, "c": {"d": {"e" : 42}}}`), `all`, `$.a`, `$.c.d.e`}, expected: true},
		{f: threePath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1, "b": 2, "c": {"d": {"e" : 42}}}`), `all`, `$.a`, `$.c.d.e`, `$.x`}, expected: false},
		{f: threePath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1, "b": 2, "c": {"d": {"e" : 42}}}`), `one`, `$.a`, `$.c.d.e`, `$.x`}, expected: true},

		// NULL inputs. Any NULL should result in NULL output.
		{f: onePath, row: sql.UntypedSqlRow{nil, `one`, `$.a`}, expected: nil},
		{f: onePath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1}`), nil, `$.a`}, expected: nil},
		{f: twoPath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1}`), `one`, `$.a`, nil}, expected: true}, // Match MySQL behavior, not docs.
		{f: twoPath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1}`), `one`, nil, `$.a`}, expected: nil},
		{f: twoPath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1}`), "all", `$.x`, nil}, expected: false}, // Match MySQL behavior, not docs.
		{f: twoPath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1}`), `all`, `$.a`, nil}, expected: nil},

		// JSON NULL documents do NOT result in NULL output.
		{f: onePath, row: sql.UntypedSqlRow{prepare(t, `null`), `all`, `$.a`}, expected: false},

		// Error cases
		{f: onePath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1}`), `None`, `$.a`}, err: errors.New("The oneOrAll argument to json_contains_path may take these values: 'one' or 'all'")},
		{f: onePath, row: sql.UntypedSqlRow{`{"a": 1`, `One`, `$.a`}, err: sql.ErrInvalidJSONText.New(1, "json_contains_path", `{"a": 1`)},
		{f: threePath, row: sql.UntypedSqlRow{prepare(t, `{"a": 1, "b": 2, "c": {"d": {"e" : 42}}}`), `one`, 42, `$.c.d.e`, `$.x`}, err: errors.New(`Invalid JSON path expression. Path must start with '$', but received: '42'`)},
	}
}
