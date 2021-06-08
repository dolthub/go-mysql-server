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
	"github.com/dolthub/go-mysql-server/sql"
)

type BindVar struct {
	Name string
}

func NewBindVar(name string) sql.Expression {
	return &BindVar{name}
}

func (bv *BindVar) Resolved() bool {
	return true
}

func (bv *BindVar) String() string {
	return "BindVar(" + bv.Name + ")"
}

func (bv *BindVar) Type() sql.Type {
	return sql.LongText
}

func (bv *BindVar) IsNullable() bool {
	return true
}

func (bv *BindVar) Eval(*sql.Context, sql.Row) (interface{}, error) {
	return nil, sql.ErrUnboundPreparedStatementVariable.New(bv.Name)
}

func (bv *BindVar) Children() []sql.Expression {
	return nil
}

func (bv *BindVar) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(bv, len(children), 0)

	}
	return bv, nil
}
