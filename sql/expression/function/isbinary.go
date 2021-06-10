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
	"bytes"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// IsBinary is a function that returns whether a blob is binary or not.
type IsBinary struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*IsBinary)(nil)

// NewIsBinary creates a new IsBinary expression.
func NewIsBinary(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &IsBinary{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (ib *IsBinary) FunctionName() string {
	return "is_binary"
}

// Eval implements the Expression interface.
func (ib *IsBinary) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	v, err := ib.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return false, nil
	}

	blob, err := sql.LongBlob.Convert(v)
	if err != nil {
		return nil, err
	}

	blobBytes := blob.(string)
	return isBinary(blobBytes), nil
}

func (ib *IsBinary) String() string {
	return fmt.Sprintf("IS_BINARY(%s)", ib.Child)
}

// WithChildren implements the Expression interface.
func (ib *IsBinary) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(ib, len(children), 1)
	}
	return NewIsBinary(ctx, children[0]), nil
}

// Type implements the Expression interface.
func (ib *IsBinary) Type() sql.Type {
	return sql.Boolean
}

const sniffLen = 8000

// isBinary detects if data is a binary value based on:
// http://git.kernel.org/cgit/git/git.git/tree/xdiff-interface.c?id=HEAD#n198
func isBinary(data string) bool {
	binData := []byte(data)
	if len(binData) > sniffLen {
		binData = binData[:sniffLen]
	}

	if bytes.IndexByte(binData, byte(0)) == -1 {
		return false
	}

	return true
}
