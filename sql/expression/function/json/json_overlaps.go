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

package json

import (

"fmt"
"github.com/dolthub/go-mysql-server/sql"
"github.com/dolthub/go-mysql-server/sql/types"
)"github.com/dolthub/go-mysql-server/sql/types"

	)

// JSONOverlaps (json_doc1, json_doc2)
//
// JSONOverlaps Compares two JSON documents. Returns true (1) if the two document have any key-value pairs or array
// elements in common. If both arguments are scalars, the function performs a simple equality test.
//
// This function serves as counterpart to JSON_CONTAINS(), which requires all elements of the array searched for to be
// present in the array searched in. Thus, JSON_CONTAINS() performs an AND operation on search keys, while
// JSON_OVERLAPS() performs an OR operation.
//
// Queries on JSON columns of InnoDB tables using JSON_OVERLAPS() in the WHERE clause can be optimized using
// multi-valued indexes. Multi-Valued Indexes, provides detailed information and examples.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-search-functions.html#function_json-overlaps
type JSONOverlaps struct {
	Left  sql.Expression
	Right sql.Expression
}

var _ sql.FunctionExpression = &JSONOverlaps{}

// NewJSONOverlaps creates a new JSONOverlaps function.
func NewJSONOverlaps(args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("JSON_OVERLAPS", "2", len(args))
	}
	return &JSONOverlaps{Left: args[0], Right: args[1]}, nil
}

// FunctionName implements sql.FunctionExpression
func (j *JSONOverlaps) FunctionName() string {
	return "json_overlaps"
}

// Description implements sql.FunctionExpression
func (j *JSONOverlaps) Description() string {
	return "compares two JSON documents, returns TRUE (1) if these have any key-value pairs or array elements in common, otherwise FALSE (0)."
}

// Resolved implements sql.Expression
func (j *JSONOverlaps) Resolved() bool {
	return j.Left.Resolved() && j.Right.Resolved()
}

// String implements sql.Expression
func (j *JSONOverlaps) String() string {
	return fmt.Sprintf("%s(%s, %s)", j.FunctionName(), j.Left.String(), j.Right.String())
}

// Type implements sql.Expression
func (j *JSONOverlaps)Type() sql.Type {
	return types.Boolean
}

// IsNullable implements sql.Expression
func (j *JSONOverlaps) IsNullable() bool {
	return j.Left.IsNullable() || j.Right.IsNullable()
}

func overlaps(left, right interface{}) bool {
	switch lVal := left.(type) {
	case nil, bool, string, float64:
		switch rVal := right.(type) {
		case []interface{}:
			for _, r := range rVal {
				if r == lVal {
					return true
				}
			}
			return false
		default:
			return lVal == rVal
		}
	case []interface{}:
		switch rVal := right.(type) {
		case nil, bool, string, float64:
			for _, l := range lVal {
				if l == rVal {
					return true
				}
			}
		case []interface{}:
			for _, l := range lVal {
				for _, r := range rVal {
					if l == r {
						return true
					}
				}
			}
			return false
		default:
			for _, l := range lVal {
				if l == rVal {
					return true
				}
			}
			return false
		}
	case map[string]interface{}:
		switch rVal := right.(type) {
		case []interface{}:
			for _, r := range rVal {
				rMap, isRMap := r.(map[string]interface{})
				if !isRMap {
					continue
				}
				// every key value pair must match
				for lk, lv := range lVal {
					rv, ok := rMap[lk]
					if !ok {
						return false
					}
					// TODO: handle further nesting?
					if lv != rv {
						return false
					}
				}
				return true
			}
		case map[string]interface{}:
			for lk, lv := range lVal {
				if rv, ok := rVal[lk]; ok && lv == rv {
					return true
				}
			}
			return false
		default:
			return false
		}
	default:
		return false
	}
	return false
}

// Eval implements sql.Expression
func (j *JSONOverlaps) Eval(ctx *sql.Context, row sql.Row) (interface{},  error) {
	span, ctx := ctx.Span(fmt.Sprintf("function.%s", j.FunctionName()))
	defer span.End()

	left, err := getJSONDocumentFromRow(ctx, row, j.Left)
	if err != nil {
		return nil, err
	}
	if left == nil {
		return nil, nil
	}

	right, err := getJSONDocumentFromRow(ctx, row, j.Right)
	if err != nil {
		return nil, err
	}
	if right == nil {
		return nil, nil
	}

	return overlaps(left.Val, right.Val), nil
}

// Children implements sql.Expression
func (j *JSONOverlaps) Children() []sql.Expression {
	return []sql.Expression{j.Left, j.Right}
}

// WithChildren implements sql.Expression
func (j *JSONOverlaps) WithChildren(children ...sql.Expression) ( sql.Expression,  error) {
	return NewJSONOverlaps(children...)
}
