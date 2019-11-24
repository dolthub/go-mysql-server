package sql

import (
	"strconv"
	"strings"
	"time"

	"gopkg.in/src-d/go-errors.v1"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

var (
	Year yearType

	ErrConvertingToYear = errors.NewKind("value %v is not a valid Year")
)

type yearType struct{}

// Compare implements Type interface.
func (t yearType) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
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

	return strings.Compare(as.(string), bs.(string)), nil
}

// Convert implements Type interface.
func (t yearType) Convert(v interface{}) (interface{}, error) {
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
			return "0000", nil
		}
		if value >= 1 && value <= 69 {
			return strconv.Itoa(int(value) + 2000), nil
		}
		if value >= 70 && value <= 99 {
			return strconv.Itoa(int(value) + 1900), nil
		}
		if value >= 1901 && value <= 2155 {
			return strconv.Itoa(int(value)), nil
		}
	case uint64:
		return t.Convert(int64(value))
	case float32:
		return t.Convert(int64(value))
	case float64:
		return t.Convert(int64(value))
	case string:
		valueLength := len(value)
		if valueLength == 1 || valueLength == 2 || valueLength == 4 {
			i, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, err
			}
			if i >= 0 && i <= 69 {
				return strconv.Itoa(int(i) + 2000), nil
			}
			if i >= 70 && i <= 99 {
				return strconv.Itoa(int(i) + 1900), nil
			}
			if i >= 1901 && i <= 2155 {
				return value, nil
			}
		}
	case time.Time:
		year := value.Year()
		if year == 0 || (year >= 1901 && year <= 2155) {
			return strconv.Itoa(year), nil
		}
	}

	return nil, ErrConvertingToYear.New(v)
}

// SQL implements Type interface.
func (t yearType) SQL(v interface{}) (sqltypes.Value, error) {
	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return sqltypes.MakeTrusted(sqltypes.Year, []byte(v.(string))), nil
}

// String implements Type interface.
func (t yearType) String() string {
	return "YEAR"
}

// Type implements Type interface.
func (t yearType) Type() query.Type {
	return sqltypes.Year
}

// Zero implements Type interface.
func (t yearType) Zero() interface{} {
	return "0000"
}