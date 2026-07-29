// Copyright 2021 Dolthub, Inc.
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

package sql

import (
	"context"
	"fmt"
	"net"
	"reflect"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

func MustConvert(val interface{}, _ ConvertInRange, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return val
}

func GetEmptyPort() (int, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return -1, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err = listener.Close(); err != nil {
		return -1, err

	}
	return port, nil
}

// FakeExtendedType is a minimal ExtendedType test double standing in for an external engine's
// type system, which resolves its own common types (via GetCommonExtendedType) and cross-type
// conversions (via ConvertToType) rather than relying on GeneralizeTypes/Type.Convert, which
// only understand GMS's built-in types. Name identifies the "type" for Equals comparisons,
// and ZeroVal is the Go zero value of its native representation (e.g. int32(0) for an
// int4-like type, float64(0) for a numeric-like type).
type FakeExtendedType struct {
	Name    string
	ZeroVal any
}

var _ ExtendedType = FakeExtendedType{}

func (f FakeExtendedType) CollationCoercibility(*Context) (collation CollationID, coercibility byte) {
	return Collation_binary, 5
}
func (f FakeExtendedType) Compare(context.Context, any, any) (int, error) { return 0, nil }
func (f FakeExtendedType) Convert(_ context.Context, v any) (any, ConvertInRange, error) {
	if reflect.TypeOf(v) == reflect.TypeOf(f.ZeroVal) {
		return v, InRange, nil
	}
	return nil, InRange, fmt.Errorf("FakeExtendedType %s: cannot convert value of type %T", f.Name, v)
}
func (f FakeExtendedType) Equals(other Type) bool {
	of, ok := other.(FakeExtendedType)
	return ok && of.Name == f.Name
}
func (f FakeExtendedType) MaxTextResponseByteLength(*Context) uint32 { return 100 }
func (f FakeExtendedType) Promote() Type                             { return f }
func (f FakeExtendedType) SQL(*Context, []byte, any) (sqltypes.Value, error) {
	return sqltypes.Value{}, nil
}
func (f FakeExtendedType) Type() query.Type        { return query.Type_VARCHAR }
func (f FakeExtendedType) ValueType() reflect.Type { return reflect.TypeOf(f.ZeroVal) }
func (f FakeExtendedType) Zero() any               { return f.ZeroVal }
func (f FakeExtendedType) String() string          { return f.Name }
func (f FakeExtendedType) SerializedCompare(context.Context, []byte, []byte) (int, error) {
	return 0, nil
}
func (f FakeExtendedType) SerializeValue(context.Context, any) ([]byte, error)   { return nil, nil }
func (f FakeExtendedType) DeserializeValue(context.Context, []byte) (any, error) { return nil, nil }
func (f FakeExtendedType) FormatValue(val any) (string, error)                   { return fmt.Sprintf("%v", val), nil }
func (f FakeExtendedType) MaxSerializedWidth() ExtendedTypeSerializedWidth {
	return ExtendedTypeSerializedWidth_64K
}

// ConvertToType simulates a real engine's numeric-family cast: an int-like value can always be
// converted into the target type's own native representation.
func (f FakeExtendedType) ConvertToType(_ *Context, _ ExtendedType, val any, _ byte) (any, ConvertInRange, error) {
	switch v := val.(type) {
	case int32:
		if _, ok := f.ZeroVal.(float64); ok {
			return float64(v), InRange, nil
		}
	case float64:
		if _, ok := f.ZeroVal.(int32); ok {
			return int32(v), InRange, nil
		}
	}
	if reflect.TypeOf(val) == reflect.TypeOf(f.ZeroVal) {
		return val, InRange, nil
	}
	return nil, InRange, fmt.Errorf("FakeExtendedType %s: cannot convert value of type %T", f.Name, val)
}
