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
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// MD5 function returns the MD5 hash of the input.
// https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_md5
type MD5 struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*MD5)(nil)

// NewMD5 returns a new MD5 function expression
func NewMD5(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &MD5{NewUnaryFunc(arg, "MD5", sql.LongText)}
}

// Eval implements sql.Expression
func (f *MD5) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	arg, err := f.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	val, err := sql.LongText.Convert(arg)
	if err != nil {
		return nil, err
	}

	h := md5.New()
	_, err = io.WriteString(h, val.(string))
	if err != nil {
		return nil, err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// WithChildren implements sql.Expression
func (f *MD5) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 1)
	}
	return NewMD5(ctx, children[0]), nil
}

// SHA1 function returns the SHA1 hash of the input.
// https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_sha1
type SHA1 struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*SHA1)(nil)

// NewSHA1 returns a new SHA1 function expression
func NewSHA1(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &SHA1{NewUnaryFunc(arg, "SHA1", sql.LongText)}
}

// Eval implements sql.Expression
func (f *SHA1) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	arg, err := f.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	val, err := sql.LongText.Convert(arg)
	if err != nil {
		return nil, err
	}

	h := sha1.New()
	_, err = io.WriteString(h, val.(string))
	if err != nil {
		return nil, err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// WithChildren implements sql.Expression
func (f *SHA1) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 1)
	}
	return NewSHA1(ctx, children[0]), nil
}

// SHA2 function returns the SHA-224/256/384/512 hash of the input.
// https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_sha2
type SHA2 struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*SHA2)(nil)

// NewSHA2 returns a new SHA2 function expression
func NewSHA2(ctx *sql.Context, arg, count sql.Expression) sql.Expression {
	return &SHA2{expression.BinaryExpression{Left: arg, Right: count}}
}

// Eval implements sql.Expression
func (f *SHA2) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	arg, err := f.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}
	countArg, err := f.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if countArg == nil {
		return nil, nil
	}

	val, err := sql.LongText.Convert(arg)
	if err != nil {
		return nil, err
	}
	count, err := sql.Int64.Convert(countArg)
	if err != nil {
		return nil, err
	}

	var h hash.Hash
	switch count.(int64) {
	case 224:
		h = sha256.New224()
	case 256, 0:
		h = sha256.New()
	case 384:
		h = sha512.New384()
	case 512:
		h = sha512.New()
	default:
		return nil, nil
	}

	_, err = io.WriteString(h, val.(string))
	if err != nil {
		return nil, err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// FunctionName implements sql.FunctionExpression
func (f *SHA2) FunctionName() string {
	return "SHA2"
}

// String implements sql.Expression
func (f *SHA2) String() string {
	return fmt.Sprintf("SHA2(%s, %s)", f.Left, f.Right)
}

// Type implements sql.Expression
func (f *SHA2) Type() sql.Type {
	return sql.LongText
}

// WithChildren implements sql.Expression
func (f *SHA2) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 2)
	}
	return NewSHA2(ctx, children[0], children[1]), nil
}
