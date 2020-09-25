package sql

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

type arrayType struct {
	underlying Type
}

// CreateArray returns a new CreateArray type of the given underlying type.
func CreateArray(underlying Type) Type {
	return arrayType{underlying}
}

func (t arrayType) Compare(a, b interface{}) (int, error) {
	a, err := t.Convert(a)
	if err != nil {
		return 0, err
	}

	b, err = t.Convert(b)
	if err != nil {
		return 0, err
	}

	left := a.([]interface{})
	right := b.([]interface{})

	if len(left) < len(right) {
		return -1, nil
	} else if len(left) > len(right) {
		return 1, nil
	}

	for i := range left {
		cmp, err := t.underlying.Compare(left[i], right[i])
		if err != nil {
			return 0, err
		}

		if cmp != 0 {
			return cmp, nil
		}
	}

	return 0, nil
}

func (t arrayType) Convert(v interface{}) (interface{}, error) {
	switch v := v.(type) {
	case []interface{}:
		var result = make([]interface{}, len(v))
		for i, v := range v {
			var err error
			result[i], err = t.underlying.Convert(v)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	case Generator:
		var values []interface{}
		for {
			val, err := v.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}

			val, err = t.underlying.Convert(val)
			if err != nil {
				return nil, err
			}

			values = append(values, val)
		}

		if err := v.Close(); err != nil {
			return nil, err
		}

		return values, nil
	default:
		return nil, ErrNotArray.New(v)
	}
}

func (t arrayType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

func (t arrayType) Promote() Type {
	return t
}

func (t arrayType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := convertForJSON(t, v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	val, err := json.Marshal(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.MakeTrusted(sqltypes.TypeJSON, val), nil
}

func (t arrayType) String() string {
	return fmt.Sprintf("ARRAY(%s)", t.underlying)
}

func (t arrayType) Type() query.Type {
	return sqltypes.TypeJSON
}

func (t arrayType) Zero() interface{} {
	return nil
}
