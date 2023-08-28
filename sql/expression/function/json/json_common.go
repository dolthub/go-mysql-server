// Copyright 2023 Dolthub, Inc.
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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func getMutableJSONVal(ctx *sql.Context, row sql.Row, json sql.Expression) (types.MutableJSONValue, error) {
	js, err := json.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if js == nil {
		return nil, nil
	}

	var converted interface{}
	switch js.(type) {
	case string, []interface{}, map[string]interface{}, types.JSONValue:
		converted, _, err = types.JSON.Convert(js)
		if err != nil {
			return nil, sql.ErrInvalidJSONText.New(js)
		}
	default:
		return nil, sql.ErrInvalidArgument.New(fmt.Sprintf("%v", js))
	}

	////  NM4 Gotta do a deep copy.

	mutable, ok := converted.(types.MutableJSONValue)
	if !ok {
		mutable, err = js.(types.JSONValue).Unmarshall(ctx)
		if err != nil {
			return nil, err
		}
	}

	return mutable, nil

}

func getSearchableJSONVal(ctx *sql.Context, row sql.Row, json sql.Expression) (types.SearchableJSONValue, error) {
	js, err := json.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if js == nil {
		return nil, nil
	}

	var converted interface{}
	switch js.(type) {
	case string, []interface{}, map[string]interface{}, types.JSONValue:
		converted, _, err = types.JSON.Convert(js)
		if err != nil {
			return nil, sql.ErrInvalidJSONText.New(js)
		}
	default:
		return nil, sql.ErrInvalidArgument.New(fmt.Sprintf("%v", js))
	}

	searchable, ok := converted.(types.SearchableJSONValue)
	if !ok {
		searchable, err = js.(types.JSONValue).Unmarshall(ctx)
		if err != nil {
			return nil, err
		}
	}

	return searchable, nil
}
