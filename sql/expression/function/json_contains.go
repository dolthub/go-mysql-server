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

package function

import (
	"github.com/dolthub/go-mysql-server/sql"
)

// JSON_CONTAINS(target, candidate[, path])
//
// JSONContains indicates by returning 1 or 0 whether a given candidate JSON document is contained within a target JSON
// document, or, if a path argument was supplied, whether the candidate is found at a specific path within the target.
// Returns NULL if any argument is NULL, or if the path argument does not identify a section of the target document.
// An error occurs if target or candidate is not a valid JSON document, or if the path argument is not a valid path
// expression or contains a * or ** wildcard. To check only whether any data exists at the path, use
// JSON_CONTAINS_PATH() instead.
//
// The following rules define containment:
//   - A candidate scalar is contained in a target scalar if and only if they are comparable and are equal. Two scalar
//     values are comparable if they have the same JSON_TYPE() types, with the exception that values of types INTEGER
//     and DECIMAL are also comparable to each other.
//
//   - A candidate array is contained in a target array if and only if every element in the candidate is contained in
//     some element of the target.
//
//   - A candidate non\
//  array is contained in a target array if and only if the candidate is contained in some element
//     of the target.
///   - A candidate object is contained in a target object if and only if for each key in the candidate there is a key
//     with the same name in the target and the value associated with the candidate key is contained in the value
//     associated with the target key.
//
// Otherwise, the candidate value is not contained in the target document.
type JSONContains struct {
	sql.Expression
}

var _ sql.FunctionExpression = JSONContains{}

// NewJSONContains creates a new JSONContains function.
func NewJSONContains(args ...sql.Expression) (sql.Expression, error) {
	return nil, ErrUnsupportedJSONFunction.New(JSONContains{}.FunctionName())
}

// FunctionName implements sql.FunctionExpression
func (j JSONContains) FunctionName() string {
	return "json_contains"
}
