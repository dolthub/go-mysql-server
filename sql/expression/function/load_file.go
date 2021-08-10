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
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
)

type LoadFile struct {
	fileName sql.Expression
}

var _ sql.FunctionExpression = (*LoadFile)(nil)

func NewLoadFile(ctx *sql.Context, fileName sql.Expression) sql.Expression {
	return &LoadFile{
		fileName: fileName,
	}
}

func (l LoadFile) Resolved() bool {
	return true
}

func (l LoadFile) String() string {
	return fmt.Sprintf("LOAD_FILE(%s)", l.fileName)
}

func (l LoadFile) Type() sql.Type {
	return sql.LongText
}

func (l LoadFile) IsNullable() bool {
	return false
}

func (l LoadFile) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	panic("implement me")
}

func (l LoadFile) Children() []sql.Expression {
	return []sql.Expression{l.fileName}
}

func (l LoadFile) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) > 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}

	return NewLoadFile(ctx, children[0]), nil
}

func (l LoadFile) FunctionName() string {
	return "load_file"
}