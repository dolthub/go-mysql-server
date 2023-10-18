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

// getMutableJSONVal returns a JSONValue from the given row and expression. The underling value is deeply copied so that
// you are free to use the mutation functions on the returned value.
// nil will be returned only if the inputs are nil. This will not return an error, so callers must check.
func getMutableJSONVal(ctx *sql.Context, row sql.Row, json sql.Expression) (types.MutableJSON, error) {
	doc, err := getJSONDocumentFromRow(ctx, row, json)
	if err != nil || doc == nil || doc.Val == nil {
		return nil, err
	}

	mutable := types.DeepCopyJson(doc.Val)
	return types.JSONDocument{Val: mutable}, nil
}

// getSearchableJSONVal returns a SearchableJSONValue from the given row and expression. The underling value is not copied
// so it is intended to be used for read-only operations.
// nil will be returned only if the inputs are nil. This will not return an error, so callers must check.
func getSearchableJSONVal(ctx *sql.Context, row sql.Row, json sql.Expression) (sql.JSONWrapper, error) {
	doc, err := getJSONDocumentFromRow(ctx, row, json)
	if err != nil || doc == nil || doc.Val == nil {
		return nil, err
	}

	return doc, nil
}

// getJSONDocumentFromRow returns a JSONDocument from the given row and expression. Helper function only intended to be
// used by functions in this file.
func getJSONDocumentFromRow(ctx *sql.Context, row sql.Row, json sql.Expression) (*types.JSONDocument, error) {
	js, err := json.Eval(ctx, row)
	if err != nil || js == nil {
		return nil, err
	}

	var converted interface{}
	switch js.(type) {
	case string, []interface{}, map[string]interface{}, sql.JSONWrapper:
		converted, _, err = types.JSON.Convert(js)
		if err != nil {
			return nil, sql.ErrInvalidJSONText.New(js)
		}
	default:
		return nil, sql.ErrInvalidArgument.New(fmt.Sprintf("%v", js))
	}

	doc, ok := converted.(types.JSONDocument)
	if !ok {
		// This should never happen, but just in case.
		doc = types.JSONDocument{Val: js.(sql.JSONWrapper).ToInterface()}
	}

	return &doc, nil
}

// pathValPair is a helper struct for use by functions which take json paths paired with a json value. eg. JSON_SET, JSON_INSERT, etc.
type pathValPair struct {
	path string
	val  sql.JSONWrapper
}

// buildPathValue builds a pathValPair from the given row and expressions. This is a common pattern in json methods to have
// pairs of arguments, and this ensures they are of the right type, non-nil, and they wrapped in a struct as a unit.
func buildPathValue(ctx *sql.Context, pathExp sql.Expression, valExp sql.Expression, row sql.Row) (*pathValPair, error) {
	path, err := pathExp.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if path == nil {
		// MySQL documented behavior is to return null, not error, if any path is null.
		return nil, nil
	}

	// make sure path is string
	if _, ok := path.(string); !ok {
		return nil, fmt.Errorf("Invalid JSON path expression")
	}

	val, err := valExp.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	jsonVal, ok := val.(sql.JSONWrapper)
	if !ok {
		jsonVal = types.JSONDocument{val}
	}

	return &pathValPair{path.(string), jsonVal}, nil
}
