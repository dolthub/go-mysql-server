// Copyright 2022 Dolthub, Inc.
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

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJSONMergePreserve(t *testing.T) {
	f2, err := NewJSONMergePreserve(
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
	)
	require.NoError(t, err)

	f3, err := NewJSONMergePreserve(
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
	)
	require.NoError(t, err)

	f4, err := NewJSONMergePreserve(
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
		expression.NewGetField(2, types.LongText, "arg3", false),
		expression.NewGetField(3, types.LongText, "arg4", false),
	)
	require.NoError(t, err)

	jsonArray1 := []interface{}{1, 2}
	jsonArray2 := []interface{}{true, false}
	jsonValue1 := `"single Value"`
	jsonObj1 := map[string]interface{}{"name": "x"}
	jsonObj2 := map[string]interface{}{"id": 47}
	jsonObj3 := map[string]interface{}{"a": 1, "b": 2}
	jsonObj4 := map[string]interface{}{"a": 3, "c": 4}
	jsonObj5 := map[string]interface{}{"a": 5, "d": 6}
	jsonObj6 := map[string]interface{}{"a": 3, "e": 8}
	jsonObj7 := map[string]interface{}{"a": map[string]interface{}{"one": false, "two": 2.55}, "e": 8}
	json3ObjResult := map[string]interface{}{"a": []interface{}{float64(1), float64(3), float64(5)}, "b": float64(2), "c": float64(4), "d": float64(6)}
	json4ObjResult := map[string]interface{}{"a": []interface{}{float64(1), float64(3), float64(5), float64(3)}, "b": float64(2), "c": float64(4), "d": float64(6), "e": float64(8)}
	sData1 := map[string]interface{}{
		"Suspect": map[string]interface{}{
			"Name":    "Bart",
			"Hobbies": []interface{}{"Skateboarding", "Mischief"},
		},
		"Victim": "Lisa",
		"Case": map[string]interface{}{
			"Id":     33845,
			"Date":   "2006-01-02T15:04:05-07:00",
			"Closed": true,
		},
	}
	sData2 := map[string]interface{}{
		"Suspect": map[string]interface{}{
			"Age":     10,
			"Parents": []interface{}{"Marge", "Homer"},
			"Hobbies": []interface{}{"Trouble"},
		},
		"Witnesses": []interface{}{"Maggie", "Ned"},
	}
	resultData := map[string]interface{}{
		"Suspect": map[string]interface{}{
			"Age":     float64(10),
			"Name":    "Bart",
			"Hobbies": []interface{}{"Skateboarding", "Mischief", "Trouble"},
			"Parents": []interface{}{"Marge", "Homer"},
		},
		"Victim": "Lisa",
		"Case": map[string]interface{}{
			"Id":     float64(33845),
			"Date":   "2006-01-02T15:04:05-07:00",
			"Closed": true,
		},
		"Witnesses": []interface{}{"Maggie", "Ned"},
	}
	mixedData := []interface{}{
		map[string]interface{}{
			"a": []interface{}{
				float64(1),
				map[string]interface{}{
					"one": false,
					"two": 2.55,
				},
			},
			"b": float64(2),
			"e": float64(8),
		},
		"single Value",
	}

	testCases := []struct {
		f        sql.Expression
		row      sql.Row
		expected interface{}
		err      error
	}{
		{f2, sql.Row{nil, nil}, types.JSONDocument{Val: []interface{}{nil, nil}}, nil},
		{f2, sql.Row{jsonArray1, nil}, types.JSONDocument{Val: []interface{}{float64(1), float64(2), nil}}, nil},
		{f2, sql.Row{jsonArray1, jsonArray2}, types.JSONDocument{Val: []interface{}{float64(1), float64(2), true, false}}, nil},
		{f2, sql.Row{jsonObj1, jsonObj2}, types.JSONDocument{Val: map[string]interface{}{"name": "x", "id": float64(47)}}, nil},
		{f2, sql.Row{1, true}, types.JSONDocument{Val: []interface{}{float64(1), true}}, nil},
		{f2, sql.Row{jsonArray1, jsonObj2}, types.JSONDocument{Val: []interface{}{float64(1), float64(2), map[string]interface{}{"id": float64(47)}}}, nil},
		{f3, sql.Row{jsonObj3, jsonObj4, jsonObj5}, types.JSONDocument{Val: json3ObjResult}, nil},
		{f2, sql.Row{sData1, sData2}, types.JSONDocument{Val: resultData}, nil},
		{f4, sql.Row{jsonObj3, jsonObj4, jsonObj5, jsonObj6}, types.JSONDocument{Val: json4ObjResult}, nil},
		{f3, sql.Row{jsonObj3, jsonObj7, jsonValue1}, types.JSONDocument{Val: mixedData}, nil},
	}

	for _, tt := range testCases {
		t.Run(tt.f.String(), func(t *testing.T) {
			require := require.New(t)
			result, err := tt.f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err == nil {
				require.NoError(err)
			} else {
				require.Equal(err.Error(), tt.err.Error())
			}

			require.Equal(tt.expected, result)
		})
	}
}
