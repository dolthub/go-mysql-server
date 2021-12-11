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
	"math"
	"strings"

	"github.com/shopspring/decimal"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"golang.org/x/text/number"

	"github.com/dolthub/go-mysql-server/sql"
)

// Format function returns a result of NumValue rounded to NumDecimalPlaces as a string.
type Format struct {
	NumValue         sql.Expression
	NumDecimalPlaces sql.Expression
	Locale           sql.Expression
}

var _ sql.FunctionExpression = (*Format)(nil)

// NewFormat returns a new Format expression.
func NewFormat(args ...sql.Expression) (sql.Expression, error) {
	var numValue, numDecimalPlaces, locale sql.Expression
	switch len(args) {
	case 2:
		numValue = args[0]
		numDecimalPlaces = args[1]
		locale = nil
	case 3:
		numValue = args[0]
		numDecimalPlaces = args[1]
		locale = args[2]
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("FORMAT", "2 or 3", len(args))
	}
	return &Format{numValue, numDecimalPlaces, locale}, nil
}

// FunctionName implements sql.FunctionExpression
func (f *Format) FunctionName() string {
	return "format"
}

// Description implements sql.FunctionExpression
func (f *Format) Description() string {
	return "returns a number formatted to specified number of decimal places."
}

// Type implements the Expression interface.
func (f *Format) Type() sql.Type { return sql.LongText }

// IsNullable implements the Expression interface.
func (f *Format) IsNullable() bool {
	return f.NumValue.IsNullable() || f.NumDecimalPlaces.IsNullable() || (f.Locale != nil && f.Locale.IsNullable())
}

func (f *Format) String() string {
	return fmt.Sprintf("format(%s, %s, %s)", f.NumValue, f.NumDecimalPlaces, f.Locale)
}

// Eval implements the Expression interface.
func (f *Format) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	numVal, err := f.NumValue.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if numVal == nil {
		return nil, nil
	}

	numDP, err := f.NumDecimalPlaces.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if numDP == nil {
		return nil, nil
	}

	locale := language.English
	if f.Locale != nil {
		loc, lErr := f.Locale.Eval(ctx, row)
		if lErr != nil {
			return nil, lErr
		}
		if loc != nil {
			locale, err = language.Parse(loc.(string))
			if err != nil {
				locale = language.English
			}
		}
	}

	numVal, err = sql.Float64.Convert(numVal)
	if err != nil {
		return nil, nil
	}
	numValue := numVal.(float64)

	numDP, err = sql.Float64.Convert(numDP)
	if err != nil {
		return nil, nil
	}
	numDecimalPlaces := numDP.(float64)
	numDecimalPlaces = math.Round(numDecimalPlaces)

	if numDecimalPlaces < 0 {
		numDecimalPlaces = 0
	}

	roundedValue := math.Round(numValue*math.Pow(10.0, numDecimalPlaces)) / math.Pow(10.0, numDecimalPlaces)

	// FORMAT(-5.932887e-08, 2);     		==> -0.00
	// FORMAT(-0.00000005932887, 2); 		==> 0.00
	// will return 0.00 for both cases
	var whole int64
	var fractionStr string
	var negative string
	if roundedValue != 0 {
		res := decimal.NewFromFloat(roundedValue)
		whole = res.IntPart()
		if whole == 0 && res.IsNegative() {
			negative = "-"
		}

		str := res.String()
		dotIdx := strings.Index(str, ".")
		if dotIdx == -1 {
			fractionStr = ""
		} else {
			fractionStr = str[dotIdx+1:]
		}
	}

	p := message.NewPrinter(locale)
	formattedWhole := p.Sprintf("%v", number.Decimal(whole))
	if numDecimalPlaces == 0 {
		return fmt.Sprintf("%s%s", negative, formattedWhole), nil
	}

	decimalChar := p.Sprintf("%v", number.Decimal(1.5))
	if len(fractionStr) < int(numDecimalPlaces) {
		rp := int(numDecimalPlaces) - len(fractionStr)
		fractionStr += strings.Repeat("0", rp)
	}

	result := fmt.Sprintf("%s%s%s%s", negative, formattedWhole, decimalChar[1:2], fractionStr)
	return result, nil
}

// Resolved implements the Expression interface.
func (f *Format) Resolved() bool {
	if f.Locale == nil {
		return f.NumValue.Resolved() && f.NumDecimalPlaces.Resolved()
	}
	return f.NumValue.Resolved() && f.NumDecimalPlaces.Resolved() && f.Locale.Resolved()
}

// Children implements the Expression interface.
func (f *Format) Children() []sql.Expression {
	if f.Locale == nil {
		return []sql.Expression{f.NumValue, f.NumDecimalPlaces}
	}
	return []sql.Expression{f.NumValue, f.NumDecimalPlaces, f.Locale}
}

// WithChildren implements the Expression interface.
func (f *Format) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if (len(children) == 2 && f.Locale == nil) || (len(children) == 3 && f.Locale != nil) {
		return NewFormat(children...)
	}
	return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 2)
}
