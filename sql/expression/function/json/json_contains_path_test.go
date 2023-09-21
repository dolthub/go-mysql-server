// Copyright 2021 Dolthub, Inc.
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

package json

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJSONContainsPath(t *testing.T) {
	// Verify arg count 3 or more.
	_, err := NewJSONContainsPath()
	require.Error(t, err)

	_, err = NewJSONContainsPath(
		expression.NewGetField(0, types.JSON, "arg1", false),
	)
	require.Error(t, err)

	_, err = NewJSONContainsPath(
		expression.NewGetField(0, types.JSON, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
	)
	require.Error(t, err)

	// setup call expressions for calling with 1, 2, and 3 paths.
	onePath, err := NewJSONContainsPath(
		expression.NewGetField(0, types.JSON, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
	)
	require.NoError(t, err)

	twoPath, err := NewJSONContainsPath(
		expression.NewGetField(0, types.JSON, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
		expression.NewGetField(3, types.LongText, "arg4", false),
	)
	require.NoError(t, err)

	threePath, err := NewJSONContainsPath(
		expression.NewGetField(0, types.JSON, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
		expression.NewGetField(3, types.LongText, "arg4", false),
		expression.NewGetField(4, types.LongText, "arg5", false),
	)
	require.NoError(t, err)

	testCases := []struct {
		fCall    sql.Expression
		input    sql.Row
		expected interface{}
		err      error
	}{
		{onePath, sql.Row{`{"a": 1, "b": 2, "c": {"d": 4}}`, `oNe`, `$.a`}, true, nil},
		{onePath, sql.Row{`{"a": 1, "b": 2, "c": {"d": 4}}`, `one`, `$.e`}, false, nil},
		{onePath, sql.Row{`{"a": 1, "b": 2, "c": {"d": 4}}`, `all`, `$.e`}, false, nil},
		{onePath, sql.Row{`{"a": 1, "b": 2, "c": {"d": 4}}`, `All`, `$.c.d`}, true, nil},

		{twoPath, sql.Row{`{"a": 1, "b": 2, "c": {"d": 4}}`, `one`, `$.a`, `$.e`}, true, nil},
		{twoPath, sql.Row{`{"a": 1, "b": 2, "c": {"d": 4}}`, `ALL`, `$.a`, `$.e`}, false, nil},

		{twoPath, sql.Row{`{"a": 1, "b": 2, "c": {"d": {"e" : 42}}}`, `all`, `$.a`, `$.c.d.e`}, true, nil},
		{threePath, sql.Row{`{"a": 1, "b": 2, "c": {"d": {"e" : 42}}}`, `all`, `$.a`, `$.c.d.e`, `$.x`}, false, nil},
		{threePath, sql.Row{`{"a": 1, "b": 2, "c": {"d": {"e" : 42}}}`, `one`, `$.a`, `$.c.d.e`, `$.x`}, true, nil},

		// NULL inputs. Any NULL should result in NULL output.
		{onePath, sql.Row{nil, `one`, `$.a`}, nil, nil},
		{onePath, sql.Row{`{"a": 1}`, nil, `$.a`}, nil, nil},
		{twoPath, sql.Row{`{"a": 1}`, `one`, `$.a`, nil}, true, nil}, // Match MySQL behavior, not docs.
		{twoPath, sql.Row{`{"a": 1}`, `one`, nil, `$.a`}, nil, nil},
		{twoPath, sql.Row{`{"a": 1}`, "all", `$.x`, nil}, false, nil}, // Match MySQL behavior, not docs.
		{twoPath, sql.Row{`{"a": 1}`, `all`, `$.a`, nil}, nil, nil},

		// Error cases
		{onePath, sql.Row{`{"a": 1}`, `None`, `$.a`}, nil, errors.New("The oneOrAll argument to json_contains_path may take these values: 'one' or 'all'")},
		{onePath, sql.Row{`{"a": 1`, `One`, `$.a`}, nil, errors.New(`Invalid JSON text: {"a": 1`)},
		{threePath, sql.Row{`{"a": 1, "b": 2, "c": {"d": {"e" : 42}}}`, `one`, 42, `$.c.d.e`, `$.x`}, nil, errors.New(`Invalid JSON path expression. Path must start with '$', but received: '42'`)},
	}

	for _, testcase := range testCases {
		t.Run(testcase.fCall.String(), func(t *testing.T) {
			require := require.New(t)
			result, err := testcase.fCall.Eval(sql.NewEmptyContext(), testcase.input)
			if testcase.err == nil {
				require.NoError(err)
			} else {
				require.Equal(err.Error(), testcase.err.Error())
			}

			require.Equal(testcase.expected, result)
		})
	}
}
