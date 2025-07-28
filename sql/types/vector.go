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
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/sql"
)

var vectorValueType = reflect.TypeOf([]float64{})

// VectorType represents the VECTOR(N) type.
// It stores a fixed-length array of N floating point numbers.
type VectorType struct {
	Dimensions int
}

var _ sql.Type = VectorType{}
var _ sql.CollationCoercible = VectorType{}

// CreateVectorType creates a VECTOR type with the specified number of dimensions.
func CreateVectorType(dimensions int) (sql.Type, error) {
	if dimensions < 1 || dimensions > 16000 {
		return nil, fmt.Errorf("VECTOR dimension must be between 1 and 16000, got %d", dimensions)
	}
	return VectorType{Dimensions: dimensions}, nil
}

// Compare implements Type interface.
func (t VectorType) Compare(ctx context.Context, a interface{}, b interface{}) (int, error) {
	if hasNulls, res := CompareNulls(a, b); hasNulls {
		return res, nil
	}

	av, _, err := t.Convert(ctx, a)
	if err != nil {
		return 0, err
	}
	bv, _, err := t.Convert(ctx, b)
	if err != nil {
		return 0, err
	}

	avec := av.([]float64)
	bvec := bv.([]float64)

	for i := 0; i < len(avec); i++ {
		if avec[i] < bvec[i] {
			return -1, nil
		} else if avec[i] > bvec[i] {
			return 1, nil
		}
	}
	return 0, nil
}

// Convert implements Type interface.
func (t VectorType) Convert(ctx context.Context, v interface{}) (interface{}, sql.ConvertInRange, error) {
	if v == nil {
		return nil, sql.InRange, nil
	}

	switch val := v.(type) {
	case []float64:
		if len(val) != t.Dimensions {
			return nil, sql.OutOfRange, fmt.Errorf("VECTOR dimension mismatch: expected %d, got %d", t.Dimensions, len(val))
		}
		return val, sql.InRange, nil
	case []interface{}:
		if len(val) != t.Dimensions {
			return nil, sql.OutOfRange, fmt.Errorf("VECTOR dimension mismatch: expected %d, got %d", t.Dimensions, len(val))
		}
		result := make([]float64, t.Dimensions)
		for i, elem := range val {
			switch e := elem.(type) {
			case float64:
				result[i] = e
			case float32:
				result[i] = float64(e)
			case int:
				result[i] = float64(e)
			case int64:
				result[i] = float64(e)
			case int32:
				result[i] = float64(e)
			default:
				if str, ok := elem.(string); ok {
					f, err := strconv.ParseFloat(str, 64)
					if err != nil {
						return nil, sql.OutOfRange, fmt.Errorf("invalid vector element: %v", elem)
					}
					result[i] = f
				} else {
					return nil, sql.OutOfRange, fmt.Errorf("invalid vector element: %v", elem)
				}
			}
		}
		return result, sql.InRange, nil
	case string:
		// Parse JSON array format: "[1.0, 2.0, 3.0]"
		val = strings.TrimSpace(val)
		if !strings.HasPrefix(val, "[") || !strings.HasSuffix(val, "]") {
			return nil, sql.OutOfRange, fmt.Errorf("VECTOR must be in JSON array format: [1.0, 2.0, ...]")
		}
		var floats []float64
		err := json.Unmarshal([]byte(val), &floats)
		if err != nil {
			return nil, sql.OutOfRange, fmt.Errorf("invalid VECTOR JSON format: %v", err)
		}
		if len(floats) != t.Dimensions {
			return nil, sql.OutOfRange, fmt.Errorf("VECTOR dimension mismatch: expected %d, got %d", t.Dimensions, len(floats))
		}
		return floats, sql.InRange, nil
	case []byte:
		// Parse JSON array format from bytes
		var floats []float64
		err := json.Unmarshal(val, &floats)
		if err != nil {
			return nil, sql.OutOfRange, fmt.Errorf("invalid VECTOR JSON format: %v", err)
		}
		if len(floats) != t.Dimensions {
			return nil, sql.OutOfRange, fmt.Errorf("VECTOR dimension mismatch: expected %d, got %d", t.Dimensions, len(floats))
		}
		return floats, sql.InRange, nil
	default:
		return nil, sql.OutOfRange, fmt.Errorf("unsupported conversion to VECTOR: %T", v)
	}
}

// MustConvert implements Type interface.
func (t VectorType) MustConvert(ctx context.Context, v interface{}) interface{} {
	value, _, err := t.Convert(ctx, v)
	if err != nil {
		panic(err)
	}
	return value
}

// Equals implements Type interface.
func (t VectorType) Equals(otherType sql.Type) bool {
	if otherVector, ok := otherType.(VectorType); ok {
		return t.Dimensions == otherVector.Dimensions
	}
	return false
}

// MaxTextResponseByteLength implements Type interface.
func (t VectorType) MaxTextResponseByteLength(ctx *sql.Context) uint32 {
	// JSON array format: "[1.0, 2.0, ...]" with about 20 chars per float
	return uint32(t.Dimensions*20 + 10)
}

// Promote implements Type interface.
func (t VectorType) Promote() sql.Type {
	return t
}

// SQL implements Type interface.
func (t VectorType) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	value, _, err := t.Convert(ctx, v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	vec := value.([]float64)
	jsonBytes, err := json.Marshal(vec)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.MakeTrusted(sqltypes.TypeJSON, jsonBytes), nil
}

// String implements Type interface.
func (t VectorType) String() string {
	return fmt.Sprintf("VECTOR(%d)", t.Dimensions)
}

// Type implements Type interface.
func (t VectorType) Type() query.Type {
	return sqltypes.TypeJSON
}

// ValueType implements Type interface.
func (t VectorType) ValueType() reflect.Type {
	return vectorValueType
}

// Zero implements Type interface.
func (t VectorType) Zero() interface{} {
	return make([]float64, t.Dimensions)
}

// CollationCoercibility implements sql.CollationCoercible interface.
func (VectorType) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}