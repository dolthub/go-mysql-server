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
	"time"
)

type ConvertTz struct {
	dt sql.Expression
	fromTz sql.Expression
	toTz sql.Expression
}

var _ sql.FunctionExpression = (*ConvertTz)(nil)

func NewConvertTz(ctx *sql.Context, dt, fromTz, toTz sql.Expression) sql.Expression {
	return &ConvertTz{
		dt: dt,
		fromTz: fromTz,
		toTz: toTz,
	}
}

func (c *ConvertTz) Resolved() bool {
	return c.dt.Resolved() && c.fromTz.Resolved() && c.toTz.Resolved()
}

func (c *ConvertTz) String() string {
	return fmt.Sprintf("CONVERT_TZ(%s, %s, %s)", c.dt, c.fromTz, c.toTz)
}

func (c *ConvertTz) Type() sql.Type {
	return sql.Datetime
}

func (c *ConvertTz) IsNullable() bool {
	return true
}

func (c *ConvertTz) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	from, err := c.fromTz.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	to, err := c.toTz.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	current, err := c.dt.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	var dt time.Time
	var dFmt string
	// Note: We could use sql.ConvertWithoutRangeCheck but we need the format of the outputted string
	for _, testFmt := range sql.TimestampDatetimeLayouts {
		if t, err := time.Parse(testFmt, current.(string)); err == nil {
			dFmt = testFmt
			dt = t
			break
		}
	}

	if dFmt == "" {
		return nil, sql.ErrConvertingToTime.New(current)
	}

	convertedFrom, err := loadLocationAndConvert(dt, from.(string))
	if err != nil {
		return nil, err
	}

	convertedTo, err := loadLocationAndConvert(convertedFrom, to.(string))
	if err != nil {
		return nil, err
	}

	return convertedTo.Format(dFmt), nil
}

func loadLocationAndConvert(t time.Time, location string) (time.Time, error) {
	loc, err := time.LoadLocation(location)
	if err != nil {
		return time.Time{}, err
	}

	return t.In(loc), nil
}

func (c *ConvertTz) Children() []sql.Expression {
	return []sql.Expression{c.dt, c.fromTz, c.toTz}
}

func (c *ConvertTz) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 3)
	}

	return NewConvertTz(ctx, children[0], children[1], children[2]), nil
}

func (c *ConvertTz) FunctionName() string {
	return "convert_tz"
}

