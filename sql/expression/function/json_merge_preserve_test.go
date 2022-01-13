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

package function

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJSONMergePreserve(t *testing.T) {
	f2, err := NewJSONMergePreserve(
		expression.NewGetField(0, sql.LongText, "arg1", false),
		expression.NewGetField(1, sql.LongText, "arg2", false),
	)
	require.NoError(t, err)

	f3, err := NewJSONMergePreserve(
		expression.NewGetField(0, sql.LongText, "arg1", false),
		expression.NewGetField(1, sql.LongText, "arg2", false),
		expression.NewGetField(2, sql.LongText, "arg3", false),
	)
	require.NoError(t, err)

	//f4, err := NewJSONMergePreserve(
	//	expression.NewGetField(0, sql.LongText, "arg1", false),
	//	expression.NewGetField(1, sql.LongText, "arg2", false),
	//	expression.NewGetField(2, sql.LongText, "arg3", false),
	//	expression.NewGetField(3, sql.LongText, "arg4", false),
	//)
	//require.NoError(t, err)

	jsonArray1 := []interface{}{1, 2}
	jsonArray2 := []interface{}{true, false}
	jsonObj1 := map[string]interface{}{"name": "x"}
	jsonObj2 := map[string]interface{}{"id": 47}
	jsonObj3 := map[string]interface{}{"a": 1, "b": 2}
	jsonObj4 := map[string]interface{}{"a": 3, "c": 4}
	jsonObj5 := map[string]interface{}{"a": 5, "d": 6}
	json3ObjResult := map[string]interface{}{"a": []interface{}{1, 3, 5}, "b": 2, "c": 4, "d": 6}
	sData1 := map[string]interface{}{
		"Suspect": map[string]interface{}{
			"Name": "Bart",
			"Hobbies": []interface{}{"Skateboarding", "Mischief"},
		},
	}
	sData2 := map[string]interface{}{
		"Suspect": map[string]interface{}{
		"Age": 10,
		"Parents": []interface{}{"Marge","Homer"},
		"Hobbies": []interface{}{"Trouble"},
		},
	}
	resultData := map[string]interface{}{
		"Suspect": map[string]interface{}{
			"Age": 10,
			"Name": "Bart",
			"Hobbies": []interface{}{"Skateboarding", "Mischief", "Trouble"},
			"Parents": []interface{}{"Marge","Homer"},
		},
	}


	testCases := []struct {
		f        sql.Expression
		row      sql.Row
		expected interface{}
		err      error
	}{
		{f2, sql.Row{jsonArray1, jsonArray2}, sql.JSONDocument{Val: []interface{}{1, 2, true, false}}, nil},
		{f2, sql.Row{jsonObj1, jsonObj2}, sql.JSONDocument{Val: map[string]interface{}{"name": "x" , "id": 47}}, nil},
		{f2, sql.Row{1, true}, sql.JSONDocument{Val: []interface{}{1, true}}, nil},
		{f2, sql.Row{jsonArray1, jsonObj2}, sql.JSONDocument{Val: []interface{}{1, 2, map[string]interface{}{"id": 47}}}, nil},
		{f3, sql.Row{jsonObj3, jsonObj4, jsonObj5}, sql.JSONDocument{Val: json3ObjResult}, nil},
		{f2, sql.Row{sData1, sData2}, sql.JSONDocument{Val: resultData}, nil},
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
