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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/encodings"
)

var (
	jsonValueType = reflect.TypeOf((*sql.JSONWrapper)(nil)).Elem()

	MaxJsonFieldByteLength = int64(1024) * int64(1024) * int64(1024)
)

var JSON sql.Type = JsonType{}
var _ sql.CollationCoercible = JsonType{}

type JsonType struct{}

// Compare implements Type interface.
func (t JsonType) Compare(ctx context.Context, a interface{}, b interface{}) (int, error) {
	if hasNulls, res := CompareNulls(a, b); hasNulls {
		return res, nil
	}
	return CompareJSON(ctx, a, b)
}

// convertJSONValue parses JSON-encoded data if the input is a string or []byte, returning the resulting JSONDocument. For
// other types, the value is returned if it can be marshalled.
func convertJSONValue(v interface{}) (interface{}, sql.ConvertInRange, error) {
	var data []byte
	var charsetMaxLength int64 = 1
	switch x := v.(type) {
	case []byte:
		data = x
	case string:
		data = []byte(x)
		charsetMaxLength = sql.Collation_Default.CharacterSet().MaxLength()
	default:
		// if |v| can be marshalled, it contains
		// a valid JSON document representation
		if b, berr := json.Marshal(v); berr == nil {
			data = b
		} else {
			return nil, sql.InRange, nil
		}
	}

	if int64(len(data))*charsetMaxLength > MaxJsonFieldByteLength {
		return nil, sql.InRange, ErrLengthTooLarge.New(len(data), MaxJsonFieldByteLength)
	}

	var val interface{}
	if err := JsonUnmarshal(data, &val); err != nil {
		return nil, sql.InRange, sql.ErrInvalidJson.New(err.Error())
	}

	return JSONDocument{Val: val}, sql.InRange, nil
}

// Convert implements Type interface.
func (t JsonType) Convert(c context.Context, v interface{}) (interface{}, sql.ConvertInRange, error) {
	switch v := v.(type) {
	case sql.JSONWrapper:
		return v, sql.InRange, nil
	case []byte:
		return convertJSONValue(v)
	case string:
		return convertJSONValue(v)
	// Text values may be stored in wrappers (e.g. Dolt's TextStorage), so unwrap to the raw string before decoding.
	case sql.StringWrapper:
		str, err := v.Unwrap(c)
		if err != nil {
			return nil, sql.InRange, err
		}
		return convertJSONValue(str)
	case int8:
		return JSONDocument{Val: int64(v)}, sql.InRange, nil
	case int16:
		return JSONDocument{Val: int64(v)}, sql.InRange, nil
	case int32:
		return JSONDocument{Val: int64(v)}, sql.InRange, nil
	case int64:
		return JSONDocument{Val: v}, sql.InRange, nil
	case uint8:
		return JSONDocument{Val: uint64(v)}, sql.InRange, nil
	case uint16:
		return JSONDocument{Val: uint64(v)}, sql.InRange, nil
	case uint32:
		return JSONDocument{Val: uint64(v)}, sql.InRange, nil
	case uint64:
		return JSONDocument{Val: v}, sql.InRange, nil
	case float32:
		return JSONDocument{Val: float64(v)}, sql.InRange, nil
	case float64:
		return JSONDocument{Val: v}, sql.InRange, nil
	case *apd.Decimal:
		return JSONDocument{Val: v}, sql.InRange, nil
	default:
		return convertJSONValue(v)
	}
}

// Equals implements the Type interface.
func (t JsonType) Equals(otherType sql.Type) bool {
	_, ok := otherType.(JsonType)
	return ok
}

// MaxTextResponseByteLength implements the Type interface
func (t JsonType) MaxTextResponseByteLength(*sql.Context) uint32 {
	return uint32(MaxJsonFieldByteLength*sql.Collation_Default.CharacterSet().MaxLength()) - 1
}

// Promote implements the Type interface.
func (t JsonType) Promote() sql.Type {
	return t
}

// SQL implements Type interface.
func (t JsonType) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	var val []byte

	// If we read the JSON from a table, pass through the bytes to avoid a deserialization and reserialization round-trip.
	// This is kind of a hack, and it means that reading JSON from tables no longer matches MySQL byte-for-byte.
	// But its worth it to avoid the round-trip, which can be very slow.
	if j, ok := v.(JSONBytes); ok {
		str, err := MarshallJson(ctx, j)
		if err != nil {
			return sqltypes.NULL, err
		}
		val = str
	} else {
		// Convert to jsonType
		jsVal, _, err := t.Convert(ctx, v)
		if err != nil {
			return sqltypes.NULL, err
		}
		js := jsVal.(sql.JSONWrapper)

		str, err := JsonToMySqlString(ctx, js)
		if err != nil {
			return sqltypes.NULL, err
		}
		val = encodings.StringToBytes(str)
	}

	return sqltypes.MakeTrusted(sqltypes.TypeJSON, val), nil
}

// String implements Type interface.
func (t JsonType) String() string {
	return "json"
}

// Type implements Type interface.
func (t JsonType) Type() query.Type {
	return sqltypes.TypeJSON
}

// ValueType implements Type interface.
func (t JsonType) ValueType() reflect.Type {
	return jsonValueType
}

// Zero implements Type interface.
func (t JsonType) Zero() interface{} {
	// MySQL throws an error for INSERT IGNORE, UPDATE IGNORE, etc. when bad json is encountered:
	// ERROR 3140 (22032): Invalid JSON text: "Invalid value." at position 0 in value for column 'table.column'.
	return nil
}

// CollationCoercibility implements sql.CollationCoercible interface.
func (JsonType) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_Default, 5
}

// DeepCopyJson implements deep copy of JSON document
func DeepCopyJson(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch v := v.(type) {
	case map[string]interface{}:
		m := v
		newMap := make(map[string]interface{})
		for k, value := range m {
			newMap[k] = DeepCopyJson(value)
		}
		return newMap
	case []interface{}:
		arr := v
		newArray := make([]interface{}, len(arr))
		for i, doc := range arr {
			newArray[i] = DeepCopyJson(doc)
		}
		return newArray
	case bool, string, float64, float32,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return v
	case *apd.Decimal:
		return new(apd.Decimal).Set(v)
	case apd.Decimal:
		return *new(apd.Decimal).Set(&v)
	default:
		return nil
	}
}

func MustJSON(s string) JSONDocument {
	var doc interface{}
	if err := JsonUnmarshal([]byte(s), &doc); err != nil {
		panic(err)
	}
	return JSONDocument{Val: doc}
}

// JsonUnmarshal unmarshals JSON data. It picks the best representation
// for each number to avoid losing precision whenever possible.
func JsonUnmarshal(data []byte, v *interface{}) error {
	if err := decodeJson(data, v); err != nil {
		return err
	}
	*v = convertJsonNumbers(*v)
	return nil
}

// JsonUnmarshalPreserveNumberPrecision unmarshals JSON data while representing every
// JSON number as an arbitrary-precision decimal. Callers that require MySQL JSON
// number normalization should use JsonUnmarshal instead.
func JsonUnmarshalPreserveNumberPrecision(data []byte, v *interface{}) error {
	if err := decodeJson(data, v); err != nil {
		return err
	}
	converted, err := convertJsonNumbersToDecimals(*v)
	if err != nil {
		return err
	}
	*v = converted
	return nil
}

// JsonUnmarshalPreserveNumberTokens unmarshals JSON while retaining each number's
// original lexical representation as a json.Number. This is useful for textual
// JSON dialects whose accepted exponent range is wider than numeric types.
func JsonUnmarshalPreserveNumberTokens(data []byte, v *interface{}) error {
	return decodeJson(data, v)
}

// JsonNumbersToDecimals recursively converts retained json.Number tokens to
// arbitrary-precision decimals without reparsing the surrounding document.
func JsonNumbersToDecimals(v interface{}) (interface{}, error) {
	return convertJsonNumbersToDecimals(v)
}

// decodeJson decodes one complete JSON value while retaining json.Number tokens.
func decodeJson(data []byte, v *interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return err
	}

	// Unlike json.Unmarshal, we need to check that the decoder has no more tokens to parse.
	// If the input was valid JSON, then we are at the end of the stream and should receive an io.EOF here.
	_, err := dec.Token()
	if err == nil {
		return fmt.Errorf("invalid JSON")
	}
	if err != io.EOF {
		return err
	}
	return nil
}

// convertJsonNumbers recursively walks a parsed JSON value and converts json.Number values to
// int64, uint64, or float64, choosing the most precise MySQL-compatible representation.
func convertJsonNumbers(v interface{}) interface{} {
	switch val := v.(type) {
	case json.Number:
		s := val.String()
		// If the number contains a decimal point or exponent, treat as float.
		f, _ := val.Float64()
		if strings.ContainsAny(s, ".eE") {
			return f
		}
		// If the number can be represented as a float without losing precision, do so.
		if math.Abs(f) < (1 << 53) {
			return f
		}
		// Try int64 first
		if i, err := val.Int64(); err == nil {
			return i
		}
		// Then try uint64
		if u, err := strconv.ParseUint(s, 10, 64); err == nil {
			return u
		}
		// Otherwise fall back to float
		return f
	case map[string]interface{}:
		for k, inner := range val {
			val[k] = convertJsonNumbers(inner)
		}
		return val
	case []interface{}:
		for i, inner := range val {
			val[i] = convertJsonNumbers(inner)
		}
		return val
	default:
		return v
	}
}

// convertJsonNumbersToDecimals recursively converts JSON number tokens to exact decimals.
func convertJsonNumbersToDecimals(v interface{}) (interface{}, error) {
	switch val := v.(type) {
	case json.Number:
		return newJSONDecimal(val.String())
	case map[string]interface{}:
		for key, inner := range val {
			converted, err := convertJsonNumbersToDecimals(inner)
			if err != nil {
				return nil, err
			}
			val[key] = converted
		}
		return val, nil
	case []interface{}:
		for i, inner := range val {
			converted, err := convertJsonNumbersToDecimals(inner)
			if err != nil {
				return nil, err
			}
			val[i] = converted
		}
		return val, nil
	default:
		return val, nil
	}
}

// newJSONDecimal parses a JSON number across the full exponent range supported by apd.
func newJSONDecimal(input string) (*apd.Decimal, error) {
	if decimal, _, err := apd.NewFromString(input); err == nil {
		return decimal, nil
	}

	negative := strings.HasPrefix(input, "-")
	unsigned := strings.TrimPrefix(input, "-")
	mantissa, exponentText, hasExponent := strings.Cut(unsigned, "e")
	if !hasExponent {
		mantissa, exponentText, hasExponent = strings.Cut(unsigned, "E")
	}
	if !hasExponent {
		return nil, fmt.Errorf("invalid JSON decimal %q", input)
	}
	exponent, err := strconv.ParseInt(exponentText, 10, 64)
	if err != nil {
		return nil, err
	}
	if dot := strings.IndexByte(mantissa, '.'); dot >= 0 {
		scale := int64(len(mantissa) - dot - 1)
		if exponent < int64(math.MinInt32)+scale || exponent > int64(math.MaxInt32)+scale {
			return nil, fmt.Errorf("JSON decimal exponent out of range")
		}
		exponent -= scale
		mantissa = mantissa[:dot] + mantissa[dot+1:]
	}
	if exponent < math.MinInt32 || exponent > math.MaxInt32 {
		return nil, fmt.Errorf("JSON decimal exponent out of range")
	}
	var coefficient apd.BigInt
	if _, ok := coefficient.SetString(mantissa, 10); !ok {
		return nil, fmt.Errorf("invalid JSON decimal %q", input)
	}
	decimal := apd.NewWithBigInt(&coefficient, int32(exponent))
	decimal.Negative = negative
	return decimal, nil
}
