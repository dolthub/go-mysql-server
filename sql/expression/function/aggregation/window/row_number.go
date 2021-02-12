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

package window

import "github.com/dolthub/go-mysql-server/sql"

type RowNumber struct {

}

var _ sql.FunctionExpression = (*RowNumber)(nil)
var _ sql.WindowAggregation = (*RowNumber)(nil)

func NewRowNumber() sql.Expression {
	return &RowNumber{}
}

func (r *RowNumber) Resolved() bool {
	return true
}

func (r *RowNumber) String() string {
	return "ROW_NUMBER()"
}

func (r *RowNumber) FunctionName() string {
	return "ROW_NUMBER"
}

func (r *RowNumber) Type() sql.Type {
	return sql.Int64
}

func (r *RowNumber) IsNullable() bool {
	return false
}

func (r *RowNumber) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return nil, nil
}

func (r *RowNumber) Children() []sql.Expression {
	return nil
}

func (r *RowNumber) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) > 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 0)
	}

	return r, nil
}

func (r *RowNumber) Add(ctx *sql.Context, row sql.Row) error {
	return nil
}

func (r *RowNumber) EvalRow(i int) (interface{}, error) {
	panic("implement me")
}

func (r *RowNumber) Finish(ctx *sql.Context) error {
	panic("implement me")
}
