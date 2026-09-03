// Copyright 2026 Dolthub, Inc.
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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

const (
	// MaxYear is the maximum supported calendar year (9999).
	MaxYear = 9999
	// MaxDayNumber is the maximum day count through 9999-12-31.
	MaxDayNumber = 3652424
)

// MakeDate constructs a date value from year and day of year.
type MakeDate struct {
	expression.BinaryExpressionStub
}

var _ sql.FunctionExpression = (*MakeDate)(nil)
var _ sql.CollationCoercible = (*MakeDate)(nil)

// NewMakeDate returns a new MakeDate expression from |year| and
// |dayOfYear| arguments, producing a [types.Date] result.
func NewMakeDate(ctx *sql.Context, year, dayOfYear sql.Expression) sql.Expression {
	return &MakeDate{expression.BinaryExpressionStub{LeftChild: year, RightChild: dayOfYear}}
}

// FunctionName implements [sql.FunctionExpression].
func (m *MakeDate) FunctionName() string { return "makedate" }

// Description implements [sql.FunctionExpression].
func (m *MakeDate) Description() string {
	return "creates a date from the year and day of year."
}

// Type implements [sql.Expression].
func (m *MakeDate) Type(ctx *sql.Context) sql.Type { return types.Date }

// CollationCoercibility implements [sql.CollationCoercible].
func (m *MakeDate) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// IsNullable implements [sql.Expression].
func (m *MakeDate) IsNullable(ctx *sql.Context) bool { return true }

// String implements [fmt.Stringer].
func (m *MakeDate) String() string {
	return fmt.Sprintf("MAKEDATE(%s, %s)", m.LeftChild, m.RightChild)
}

// WithChildren implements [sql.Expression]. It returns a copy of
// MakeDate with new |children| expressions, or an error if the
// count of |children| is not exactly 2.
func (m *MakeDate) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 2)
	}
	return NewMakeDate(ctx, children[0], children[1]), nil
}

// Eval implements [sql.Expression].
func (m *MakeDate) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	day, ok, err := evalInt64(ctx, m.RightChild, row)
	if err != nil || !ok {
		return nil, err
	}
	year, ok, err := evalInt64(ctx, m.LeftChild, row)
	if err != nil || !ok {
		return nil, err
	}

	if year < 0 || year > MaxYear || day <= 0 || day > MaxDayNumber {
		return nil, nil
	}

	if year < types.TwoDigitYearCutoff {
		year = types.TwoDigitYear(year)
	}

	res := time.Date(int(year), time.January, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(day-1))
	if res.Year() > MaxYear || res.Year() < 0 {
		return nil, nil
	}
	return res, nil
}
