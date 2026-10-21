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
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type cmpDir uint8

const (
	cmpGreater cmpDir = iota
	cmpLess
)

// compEval is used to implement Greatest/Least Eval() using an explicit comparison direction
func compEval(
	ctx *sql.Context,
	args []sql.Expression,
	row sql.Row,
	cmpType, retType sql.Type,
	direction cmpDir,
) (any, error) {
	if retType == types.Null {
		return nil, nil
	}
	if cmpType == nil {
		cmpType = retType
	}
	var res any
	for _, arg := range args {
		val, err := arg.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}

		v, _, err := cmpType.Convert(ctx, val)
		if err != nil {
			// TODO: warn
		}
		if res == nil {
			res = v
			continue
		}
		if v == nil {
			continue
		}
		cmp, err := cmpType.Compare(ctx, v, res)
		if direction == cmpLess {
			cmp = -cmp
		}
		if cmp > 0 {
			res = v
		}
	}
	res, _, err := types.TypeAwareConversion(ctx, res, cmpType, retType)
	if err != nil {
		// TODO: warn
	}
	return res, nil
}

// cmpRetType is used to determine the comparison and return type from args based on the rules described for
// Greatest/Least.
func cmpRetType(ctx *sql.Context, args ...sql.Expression) (sql.Type, sql.Type, error) {
	var hasString bool
	var hasFloat bool
	var hasDouble bool
	var hasDecimal bool
	var hasInt bool
	var hasUint bool
	var hasUint32 bool
	var hasInt64 bool
	var hasUint64 bool
	var hasDate bool
	var hasTime bool
	var hasDatetime bool // same as timestamp
	var decIntDigits, decScale uint8
	var dtPrec int
	for _, arg := range args {
		if !arg.Resolved() {
			return nil, nil, nil
		}
		argType := arg.Type(ctx)
		if svt, ok := argType.(sql.SystemVariableType); ok {
			argType = svt.UnderlyingType()
		}

		switch {
		// If ANY argument is NULL, the resulting type will be NULL
		case argType == types.Null:
			return types.Null, nil, nil
		case argType == types.Int8 || argType == types.Boolean:
			hasInt = true
			decIntDigits = max(decIntDigits, 3)
		case argType == types.Int16:
			hasInt = true
			decIntDigits = max(decIntDigits, 5)
		case argType == types.Int24:
			hasInt = true
			decIntDigits = max(decIntDigits, 8)
		case argType == types.Int32:
			hasInt = true
			decIntDigits = max(decIntDigits, 10)
		case argType == types.Int64:
			hasInt64 = true
			decIntDigits = max(decIntDigits, 19)
		case argType == types.Uint8:
			hasUint = true
			decIntDigits = max(decIntDigits, 3)
		case argType == types.Uint16:
			hasUint = true
			decIntDigits = max(decIntDigits, 5)
		case argType == types.Uint24:
			hasUint = true
			decIntDigits = max(decIntDigits, 8)
		case argType == types.Uint32:
			hasUint32 = true
			decIntDigits = max(decIntDigits, 10)
		case argType == types.Uint64:
			hasUint64 = true
			decIntDigits = max(decIntDigits, 20)
		case argType == types.Float32:
			hasFloat = true
		case argType == types.Float64:
			hasDouble = true
		case types.IsDecimal(argType):
			hasDecimal = true
			decType := argType.(sql.DecimalType)
			prec, scale := decType.Precision(), decType.Scale()
			decIntDigits = max(decIntDigits, prec-scale)
			decScale = max(decScale, scale)
		case types.IsDateType(argType):
			hasDate = true
		case types.IsTimespan(argType):
			hasTime = true
			dtPrec = max(dtPrec, argType.(types.TimespanType_).Precision())
		case types.IsDatetimeType(argType) || types.IsTimestampType(argType):
			hasDatetime = true
			dtPrec = max(dtPrec, argType.(sql.DatetimeType).Precision())
		case types.IsText(argType):
			hasString = true
		case types.IsTuple(argType):
			return nil, nil, sql.ErrInvalidType.New("tuple")
		default:
			return nil, nil, ErrUnsupportedType.New(argType)
		}
	}

	// Special case for DATE/DATETIME types. Does not apply to TIMESTAMP type.
	// If there is DATETIME type or a combination of DATE and TIME types, the comparison type is DATETIME.
	// If there is a DATE type, the comparison type is DATE.
	// Regardless of the presence of other types, all arguments are converted to and compared as the corresponding
	// type, then converted to the result type (determined in remaining logic).
	var cmpType sql.Type
	if hasDatetime || (hasDate && hasTime) {
		cmpType = types.MustCreateDatetimeType(sqltypes.Datetime, dtPrec)
	} else if hasDate {
		cmpType = types.Date
	}

	// If ANY argument is a string, the resulting type will be TEXT
	if hasString {
		return types.LongText, cmpType, nil
	}

	// If there is a mix of NUMERIC and TEMPORAL types, the result is TEXT
	hasSigned := hasInt || hasInt64
	hasUnsigned := hasUint || hasUint32 || hasUint64
	hasNumeric := hasSigned || hasUnsigned || hasFloat || hasDouble || hasDecimal
	hasTemporal := hasDate || hasTime || hasDatetime
	if hasNumeric && hasTemporal {
		return types.LongText, cmpType, nil
	}
	// If any argument is DOUBLE or a combination of FLOAT and DECIMAL, the result is DOUBLE
	if hasDouble || (hasFloat && hasDecimal) {
		return types.Float64, cmpType, nil
	}
	// If any argument is FLOAT, the result is FLOAT
	if hasFloat {
		return types.Float32, cmpType, nil
	}
	// If any argument is DECIMAL or a combination of SIGNED INTEGER and UNSIGNED BIG INTEGER, the result is DECIMAL
	if hasDecimal || (hasSigned && hasUint64) {
		decPrec := min(decIntDigits+decScale, types.DecimalTypeMaxPrecision)
		decScale = min(decScale, types.DecimalTypeMaxScale)
		return types.MustCreateDecimalType(decPrec, decScale), cmpType, nil
	}
	// If any argument is UNSIGNED BIG INTEGER, the result is UNSIGNED BIG INTEGER
	if hasUint64 {
		return types.Uint64, cmpType, nil
	}
	// TODO: For uint32 + signed int, MySQL returns int32 type, which can result in out of range errors.
	//  This is a bug on MySQL's part.
	// If any argument is BIG INTEGER or a combination of SIGNED AND UNSIGNED INTEGER,
	// the result is SIGNED BIG INTEGER
	if hasInt64 || (hasSigned && hasUint32) {
		return types.Int64, cmpType, nil
	}
	// If all arguments are UNSIGNED INTEGER, the result is UNSIGNED INTEGER
	if hasUnsigned && !hasSigned {
		return types.Uint32, cmpType, nil
	}
	// If all arguments are UNSIGNED or SIGNED INTEGER, the result is SIGNED INTEGER
	if hasSigned {
		return types.Int32, cmpType, nil
	}

	// If there is DATETIME or a combination of DATE and TIME, the result is DATETIME
	if hasDatetime || (hasDate && hasTime) {
		return types.MustCreateDatetimeType(sqltypes.Datetime, dtPrec), cmpType, nil
	}
	// If all arguments are TIME, the result is TIME
	if hasTime {
		return types.MustCreateDatetimeType(sqltypes.Time, dtPrec), cmpType, nil
	}
	// If all arguments are DATE, the result is DATE
	if hasDate {
		return types.Date, cmpType, nil
	}

	// Should not be possible, but just in case the fallback comparison type is TEXT
	return types.LongText, cmpType, nil
}

// Greatest returns the argument with the greatest value as determined by cmpType.
type Greatest struct {
	Args             []sql.Expression
	retType, cmpType sql.Type
}

var _ sql.FunctionExpression = (*Greatest)(nil)

// ErrUnsupportedType is returned when an argument to Greatest or Latest is not numeric or string
var ErrUnsupportedType = errors.NewKind("unsupported type for greatest/least argument: %T")

// NewGreatest creates a new Greatest UDF
func NewGreatest(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	if len(args) <= 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("GREATEST", "2 or more", len(args))
	}
	retType, cmpType, err := cmpRetType(ctx, args...)
	if err != nil {
		return nil, err
	}
	return &Greatest{
		Args:    args,
		retType: retType,
		cmpType: cmpType,
	}, nil
}

// FunctionName implements sql.FunctionExpression
func (f *Greatest) FunctionName() string {
	return "greatest"
}

// Description implements sql.FunctionExpression
func (f *Greatest) Description() string {
	return "returns the greatest numeric or string value."
}

// Type implements the Expression interface.
func (f *Greatest) Type(ctx *sql.Context) sql.Type {
	if f.retType != nil {
		return f.retType
	}
	return f.Args[0].Type(ctx)
}

// IsNullable implements the Expression interface.
func (f *Greatest) IsNullable(ctx *sql.Context) bool {
	for _, arg := range f.Args {
		if arg.IsNullable(ctx) {
			return true
		}
	}
	return false
}

func (f *Greatest) String() string {
	var args = make([]string, len(f.Args))
	for i, arg := range f.Args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("%s(%s)", f.FunctionName(), strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (f *Greatest) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewGreatest(ctx, children...)
}

// Resolved implements the Expression interface.
func (f *Greatest) Resolved() bool {
	for _, arg := range f.Args {
		if !arg.Resolved() {
			return false
		}
	}
	return f.retType != nil
}

// Children implements the Expression interface.
func (f *Greatest) Children() []sql.Expression { return f.Args }

// Eval implements the Expression interface.
func (f *Greatest) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	return compEval(ctx, f.Args, row, f.cmpType, f.retType, cmpGreater)
}

// Least returns the argument with the greatest value as determined by retType.
type Least struct {
	Args             []sql.Expression
	retType, cmpType sql.Type
}

var _ sql.FunctionExpression = (*Least)(nil)

// NewLeast creates a new Least UDF
func NewLeast(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	if len(args) <= 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("LEAST", "2 or more", len(args))
	}
	retType, cmpType, err := cmpRetType(ctx, args...)
	if err != nil {
		return nil, err
	}
	return &Least{
		Args:    args,
		retType: retType,
		cmpType: cmpType,
	}, nil
}

// FunctionName implements sql.FunctionExpression
func (f *Least) FunctionName() string {
	return "least"
}

// Description implements sql.FunctionExpression
func (f *Least) Description() string {
	return "returns the smaller numeric or string value."
}

// Type implements the Expression interface.
func (f *Least) Type(ctx *sql.Context) sql.Type {
	if f.retType != nil {
		return f.retType
	}
	return f.Args[0].Type(ctx)
}

// IsNullable implements the Expression interface.
func (f *Least) IsNullable(ctx *sql.Context) bool {
	for _, arg := range f.Args {
		if arg.IsNullable(ctx) {
			return true
		}
	}
	return false
}

func (f *Least) String() string {
	var args = make([]string, len(f.Args))
	for i, arg := range f.Args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("%s(%s)", f.FunctionName(), strings.Join(args, ", "))
}

// WithChildren implements the Expression interface.
func (f *Least) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewLeast(ctx, children...)
}

// Resolved implements the Expression interface.
func (f *Least) Resolved() bool {
	for _, arg := range f.Args {
		if !arg.Resolved() {
			return false
		}
	}
	return f.retType != nil
}

// Children implements the Expression interface.
func (f *Least) Children() []sql.Expression { return f.Args }

// Eval implements the Expression interface.
func (f *Least) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	return compEval(ctx, f.Args, row, f.cmpType, f.retType, cmpLess)
}
