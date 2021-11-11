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
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"math"
	"strings"
)

// Format function returns a result of numX rounded to df decimal places as a string.
type Format struct {
	Val sql.Expression
	Df sql.Expression
	Locale sql.Expression
}

var _ sql.FunctionExpression = (*Format)(nil)

// NewFormat returns a new Format expression.
func NewFormat(args ...sql.Expression) (sql.Expression, error) {
	var numX, df, locale sql.Expression
	switch len(args) {
	case 2:
		numX = args[0]
		df = args[1]
		locale = nil
	case 3:
		numX = args[0]
		df = args[1]
		locale = args[2]
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("FORMAT", "2 or 3", len(args))
	}
	return &Format{numX, df, locale}, nil
}

// FunctionName implements sql.FunctionExpression
func (f *Format) FunctionName() string {
	return "format"
}

// Type implements the Expression interface.
func (f *Format) Type() sql.Type { return sql.LongText }

// IsNullable implements the Expression interface.
func (f *Format) IsNullable() bool {
	return f.Val.IsNullable() || f.Df.IsNullable() || f.Locale.IsNullable()
}

func (f *Format) String() string {
	return fmt.Sprintf("format(%s, %s, %s)", f.Val, f.Df, f.Locale)
}

// Eval implements the Expression interface.
func (f *Format) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	value, err := f.Val.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	if !sql.IsNumber(f.Val.Type()) {
		value, err = sql.Float64.Convert(value)
		if err != nil {
			return nil, nil
		}
	}

	var dVal float64
	dTemp, err := f.Df.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if dTemp == nil {
		return nil, nil
	}

	switch dNum := dTemp.(type) {
	case float64:
		dVal = float64(int64(math.Round(dNum)))
	case float32:
		dVal = float64(int64(math.Round(float64(dNum))))
	case int64:
		dVal = float64(dNum)
	case int32:
		dVal = float64(dNum)
	case int16:
		dVal = float64(dNum)
	case int8:
		dVal = float64(dNum)
	case uint64:
		dVal = float64(dNum)
	case uint32:
		dVal = float64(dNum)
	case uint16:
		dVal = float64(dNum)
	case uint8:
		dVal = float64(dNum)
	case int:
		dVal = float64(dNum)
	default:
		dTemp, err = sql.Float64.Convert(dTemp)
		if err == nil {
			dVal = dTemp.(float64)
		} else {
			return nil, nil
		}
	}

	if dVal < float64(0) {
		dVal = float64(0)
	}
	xVal := math.Round(value.(float64)*math.Pow(10.0, dVal)) / math.Pow(10.0, dVal)

	xValStr := fmt.Sprintf("%f", xVal)
	s := strings.Split(xValStr, ".")
	argLen := len(s)
	var whole string
	var decimal string
	if argLen > 2 || argLen < 1 {
		return nil, nil
	} else if argLen == 2 {
		whole = s[0]
		decimal = s[1]
	} else {
		whole = s[0]
		decimal = ""
	}

	var i int
	_, err = fmt.Sscanf(whole, "%d", &i)
	p := message.NewPrinter(language.English)


	if dVal == 0 {
		return fmt.Sprintf("%s", p.Sprintf("%d", i)), nil
	}

	if len(decimal) < int(dVal) {
		rp := int(dVal) - len(decimal)
		decimal += strings.Repeat("0", rp)
	}


	result := fmt.Sprintf("%s.%s", p.Sprintf("%d", i), decimal[:int(dVal)])
	return result, nil
}

// Resolved implements the Expression interface.
func (f *Format) Resolved() bool {
	return f.Val.Resolved() && f.Df.Resolved() && f.Locale.Resolved()
}

// Children implements the Expression interface.
func (f *Format) Children() []sql.Expression {
	if f.Locale == nil {
		return []sql.Expression{f.Val, f.Df}
	}
	return []sql.Expression{f.Val, f.Df, f.Locale}
}

// WithChildren implements the Expression interface.
func (*Format) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewFormat(children...)
}
