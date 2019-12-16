package sql

import (
	"bytes"
	"encoding/json"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

var JSON JsonType = jsonType{}

type JsonType interface {
	Type
}

type jsonType struct{}

// Compare implements Type interface.
func (t jsonType) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}
	//TODO: this won't work if a JSON has two fields in a different order
	return bytes.Compare(a.([]byte), b.([]byte)), nil
}

// Convert implements Type interface.
func (t jsonType) Convert(v interface{}) (interface{}, error) {
	switch v := v.(type) {
	case string:
		var doc interface{}
		if err := json.Unmarshal([]byte(v), &doc); err != nil {
			return json.Marshal(v)
		}
		return json.Marshal(doc)
	case []byte:
		var doc interface{}
		if err := json.Unmarshal(v, &doc); err != nil {
			return json.Marshal(v)
		}
		return json.Marshal(doc)
	default:
		return json.Marshal(v)
	}
}

// MustConvert implements the Type interface.
func (t jsonType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Promote implements the Type interface.
func (t jsonType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t jsonType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.MakeTrusted(sqltypes.TypeJSON, v.([]byte)), nil
}

// String implements Type interface.
func (t jsonType) String() string {
	return "JSON"
}

// Type implements Type interface.
func (t jsonType) Type() query.Type {
	return sqltypes.TypeJSON
}

// Zero implements Type interface.
func (t jsonType) Zero() interface{} {
	return []byte(`""`)
}
