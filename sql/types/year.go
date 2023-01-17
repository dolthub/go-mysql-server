// Copyright 2022 Dolthub, Inc.
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

package types

import (
	"reflect"
	"strconv"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

var (
	Year sql.YearType = YearType_{}

	ErrConvertingToYear = errors.NewKind("value %v is not a valid Year")

	yearValueType = reflect.TypeOf(int16(0))
)

type YearType_ struct{}

// Compare implements Type interface.
func (t YearType_) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := CompareNulls(a, b); hasNulls {
		return res, nil
	}

	as, err := t.Convert(a)
	if err != nil {
		return 0, err
	}
	bs, err := t.Convert(b)
	if err != nil {
		return 0, err
	}
	ai := as.(int16)
	bi := bs.(int16)

	if ai == bi {
		return 0, nil
	}
	if ai < bi {
		return -1, nil
	}
	return 1, nil
}

// Convert implements Type interface.
func (t YearType_) Convert(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	switch value := v.(type) {
	case int:
		return t.Convert(int64(value))
	case uint:
		return t.Convert(int64(value))
	case int8:
		return t.Convert(int64(value))
	case uint8:
		return t.Convert(int64(value))
	case int16:
		return t.Convert(int64(value))
	case uint16:
		return t.Convert(int64(value))
	case int32:
		return t.Convert(int64(value))
	case uint32:
		return t.Convert(int64(value))
	case int64:
		if value == 0 {
			return int16(0), nil
		}
		if value >= 1 && value <= 69 {
			return int16(value + 2000), nil
		}
		if value >= 70 && value <= 99 {
			return int16(value + 1900), nil
		}
		if value >= 1901 && value <= 2155 {
			return int16(value), nil
		}
	case uint64:
		return t.Convert(int64(value))
	case float32:
		return t.Convert(int64(value))
	case float64:
		return t.Convert(int64(value))
	case decimal.Decimal:
		return t.Convert(value.IntPart())
	case decimal.NullDecimal:
		if !value.Valid {
			return nil, nil
		}
		return t.Convert(value.Decimal.IntPart())
	case string:
		valueLength := len(value)
		if valueLength == 1 || valueLength == 2 || valueLength == 4 {
			i, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, err
			}
			if i == 0 {
				return int16(2000), nil
			}
			return t.Convert(i)
		}
	case time.Time:
		year := value.Year()
		if year == 0 || (year >= 1901 && year <= 2155) {
			return int16(year), nil
		}
	}

	return nil, ErrConvertingToYear.New(v)
}

// MustConvert implements the Type interface.
func (t YearType_) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Equals implements the Type interface.
func (t YearType_) Equals(otherType sql.Type) bool {
	_, ok := otherType.(YearType_)
	return ok
}

// MaxTextResponseByteLength implements the Type interface
func (t YearType_) MaxTextResponseByteLength() uint32 {
	return 4
}

// Promote implements the Type interface.
func (t YearType_) Promote() sql.Type {
	return t
}

// SQL implements Type interface.
func (t YearType_) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	stop := len(dest)
	dest = strconv.AppendInt(dest, int64(v.(int16)), 10)
	val := dest[stop:]

	return sqltypes.MakeTrusted(sqltypes.Year, val), nil
}

// String implements Type interface.
func (t YearType_) String() string {
	return "year"
}

// Type implements Type interface.
func (t YearType_) Type() query.Type {
	return sqltypes.Year
}

// ValueType implements Type interface.
func (t YearType_) ValueType() reflect.Type {
	return yearValueType
}

// Zero implements Type interface.
func (t YearType_) Zero() interface{} {
	return int16(0)
}
