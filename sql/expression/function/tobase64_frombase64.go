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

package function

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// ToBase64 is a function to encode a string to the Base64 format
// using the same dialect that MySQL's TO_BASE64 uses
type ToBase64 struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*ToBase64)(nil)

// NewToBase64 creates a new ToBase64 expression.
func NewToBase64(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &ToBase64{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (t *ToBase64) FunctionName() string {
	return "to_base64"
}

// Eval implements the Expression interface.
func (t *ToBase64) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	str, err := t.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if str == nil {
		return nil, nil
	}

	str, err = sql.LongText.Convert(str)
	if err != nil {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(str))
	}

	encoded := base64.StdEncoding.EncodeToString([]byte(str.(string)))

	lenEncoded := len(encoded)
	if lenEncoded <= 76 {
		return encoded, nil
	}

	// Split into max 76 chars lines
	var out strings.Builder
	start := 0
	end := 76
	for {
		out.WriteString(encoded[start:end] + "\n")
		start += 76
		end += 76
		if end >= lenEncoded {
			out.WriteString(encoded[start:lenEncoded])
			break
		}
	}

	return out.String(), nil
}

// String implements the fmt.Stringer interface.
func (t *ToBase64) String() string {
	return fmt.Sprintf("TO_BASE64(%s)", t.Child)
}

// IsNullable implements the Expression interface.
func (t *ToBase64) IsNullable() bool {
	return t.Child.IsNullable()
}

// WithChildren implements the Expression interface.
func (t *ToBase64) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}
	return NewToBase64(ctx, children[0]), nil
}

// Type implements the Expression interface.
func (t *ToBase64) Type() sql.Type {
	return sql.LongText
}

// FromBase64 is a function to decode a Base64-formatted string
// using the same dialect that MySQL's FROM_BASE64 uses
type FromBase64 struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*FromBase64)(nil)

// NewFromBase64 creates a new FromBase64 expression.
func NewFromBase64(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &FromBase64{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (t *FromBase64) FunctionName() string {
	return "from_base64"
}

// Eval implements the Expression interface.
func (t *FromBase64) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	str, err := t.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if str == nil {
		return nil, nil
	}

	str, err = sql.LongText.Convert(str)
	if err != nil {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(str))
	}

	decoded, err := base64.StdEncoding.DecodeString(str.(string))
	if err != nil {
		return nil, err
	}

	return string(decoded), nil
}

// String implements the fmt.Stringer interface.
func (t *FromBase64) String() string {
	return fmt.Sprintf("FROM_BASE64(%s)", t.Child)
}

// IsNullable implements the Expression interface.
func (t *FromBase64) IsNullable() bool {
	return t.Child.IsNullable()
}

// WithChildren implements the Expression interface.
func (t *FromBase64) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}
	return NewFromBase64(ctx, children[0]), nil
}

// Type implements the Expression interface.
func (t *FromBase64) Type() sql.Type {
	return sql.LongText
}
