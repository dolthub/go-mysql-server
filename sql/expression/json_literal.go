// Copyright 2020-2021 Dolthub, Inc.
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

package expression

import (
	"encoding/json"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// JSONLiteral represents a JSON string literal.
type JSONLiteral struct {
	json []byte
}

// NewJSONLiteral creates a new JSONLiteral expression.
func NewJSONLiteral(json string) JSONLiteral {
	return JSONLiteral{ []byte(json)}
}

// Resolved implements the Expression interface.
func (l JSONLiteral) Resolved() bool {
	return true
}

// IsNullable implements the Expression interface.
func (l JSONLiteral) IsNullable() bool {
	return false
}

// Type implements the Expression interface.
func (l JSONLiteral) Type() sql.Type {
	return sql.JSON
}

// Eval implements the Expression interface.
func (l JSONLiteral) Eval(ctx *sql.Context, row sql.Row) (doc interface{}, err error) {
	if err = json.Unmarshal(l.json, &doc); err != nil {
		return nil, sql.ErrInvalidJSONText.New(err.Error())
	}
	return doc, nil
}

func (l JSONLiteral) String() string {
	return string(l.json)
}

func (l JSONLiteral) DebugString() string {
	return fmt.Sprintf("JSON(%s)", string(l.json))
}

// WithChildren implements the Expression interface.
func (l JSONLiteral) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 0)
	}
	return l, nil
}

// Children implements the Expression interface.
func (JSONLiteral) Children() []sql.Expression {
	return nil
}

func JSONLiteralFromLiteral(literal *Literal) (JSONLiteral, error) {
	if _, ok := literal.Type().(sql.StringType); !ok {
		return JSONLiteral{}, sql.ErrInvalidJSONText.New(literal.String())
	}
	s := literal.Value().(string)
	return JSONLiteral{json: []byte(s)}, nil
}