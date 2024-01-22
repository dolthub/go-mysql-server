// Copyright 2024 Dolthub, Inc.
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
	"sync"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/sql"
)

//TODO: doc
type Custom interface {
	sql.Type
	GetStructure() CustomStructure
	WithStructure(newStructure CustomStructure) Custom
	SerializeValue(val any) ([]byte, error)
	DeserializeValue(val []byte) (any, error)
	FormatValue(val any) (string, error)
	FormatSerializedValue(val []byte) (string, error)
	SerializeType() uint8
}

//TODO: doc
type CustomFunctions struct {
	Compare                   func(c Custom, v1 any, v2 any) (int, error)
	Convert                   func(c Custom, val any) (any, sql.ConvertInRange, error)
	Equals                    func(c Custom, otherType sql.Type) bool
	MaxTextResponseByteLength func(ctx *sql.Context, c Custom) uint32
	Promote                   func(c Custom) CustomStructure
	SQL                       func(ctx *sql.Context, c Custom, dest []byte, val any) (sqltypes.Value, error)
	Type                      func(c Custom) query.Type
	ValueType                 func(c Custom) reflect.Type
	Zero                      func(c Custom) any
	String                    func(c Custom) string
}

//TODO: doc
type CustomStructure interface {
	SerializeValue(c Custom, val any) ([]byte, error)
	DeserializeValue(c Custom, val []byte) (any, error)
	FormatValue(c Custom, val any) (string, error)
}

//TODO: doc
type customDefinition struct {
	id        uint8
	functions customFunctionRefs
	structure CustomStructure
}

//TODO: doc
type customFunctionRefs struct {
	Compare                   uint32
	Convert                   uint32
	Equals                    uint32
	MaxTextResponseByteLength uint32
	Promote                   uint32
	SQL                       uint32
	Type                      uint32
	ValueType                 uint32
	Zero                      uint32
	String                    uint32
}

var (
	cEncodedTypes                   []Custom
	cFuncsCompare                   []func(Custom, any, any) (int, error)
	cFuncsConvert                   []func(Custom, any) (any, sql.ConvertInRange, error)
	cFuncsEquals                    []func(Custom, sql.Type) bool
	cFuncsMaxTextResponseByteLength []func(*sql.Context, Custom) uint32
	cFuncsPromote                   []func(Custom) CustomStructure
	cFuncsSQL                       []func(*sql.Context, Custom, []byte, any) (sqltypes.Value, error)
	cFuncsType                      []func(Custom) query.Type
	cFuncsValueType                 []func(Custom) reflect.Type
	cFuncsZero                      []func(Custom) any
	cFuncsString                    []func(Custom) string

	customTypeMutex = &sync.RWMutex{}
)

var _ Custom = customDefinition{}

//TODO: doc
func RegisterCustomType(defaultStructure CustomStructure, functions CustomFunctions) Custom {
	customTypeMutex.Lock()
	defer customTypeMutex.Unlock()

	if len(cEncodedTypes) >= 255 {
		panic("too many custom types")
	}
	cFuncsCompare = append(cFuncsCompare, functions.Compare)
	cFuncsConvert = append(cFuncsConvert, functions.Convert)
	cFuncsEquals = append(cFuncsEquals, functions.Equals)
	cFuncsMaxTextResponseByteLength = append(cFuncsMaxTextResponseByteLength, functions.MaxTextResponseByteLength)
	cFuncsPromote = append(cFuncsPromote, functions.Promote)
	cFuncsSQL = append(cFuncsSQL, functions.SQL)
	cFuncsType = append(cFuncsType, functions.Type)
	cFuncsValueType = append(cFuncsValueType, functions.ValueType)
	cFuncsZero = append(cFuncsZero, functions.Zero)
	cFuncsString = append(cFuncsString, functions.String)
	c := customDefinition{
		id: uint8(len(cEncodedTypes)),
		functions: customFunctionRefs{
			Compare:                   uint32(len(cFuncsCompare) - 1),
			Convert:                   uint32(len(cFuncsConvert) - 1),
			Equals:                    uint32(len(cFuncsEquals) - 1),
			MaxTextResponseByteLength: uint32(len(cFuncsMaxTextResponseByteLength) - 1),
			Promote:                   uint32(len(cFuncsPromote) - 1),
			SQL:                       uint32(len(cFuncsSQL) - 1),
			Type:                      uint32(len(cFuncsType) - 1),
			ValueType:                 uint32(len(cFuncsValueType) - 1),
			Zero:                      uint32(len(cFuncsZero) - 1),
			String:                    uint32(len(cFuncsString) - 1),
		},
		structure: defaultStructure,
	}
	cEncodedTypes = append(cEncodedTypes, c)
	return c
}

//TODO: doc
func DeserializeCustomType(serializedType uint8) (Custom, bool) {
	customTypeMutex.RLock()
	defer customTypeMutex.RUnlock()

	if serializedType >= uint8(len(cEncodedTypes)) {
		return nil, false
	}
	return cEncodedTypes[serializedType], true
}

// GetStructure implements the Custom interface.
func (c customDefinition) GetStructure() CustomStructure {
	return c.structure
}

// WithStructure implements the Custom interface.
func (c customDefinition) WithStructure(newStructure CustomStructure) Custom {
	return customDefinition{
		functions: c.functions,
		structure: newStructure,
	}
}

// SerializeValue implements the Custom interface.
func (c customDefinition) SerializeValue(val any) ([]byte, error) {
	return c.structure.SerializeValue(c, val)
}

// DeserializeValue implements the Custom interface.
func (c customDefinition) DeserializeValue(val []byte) (any, error) {
	return c.structure.DeserializeValue(c, val)
}

// FormatValue implements the Custom interface.
func (c customDefinition) FormatValue(val any) (string, error) {
	return c.structure.FormatValue(c, val)
}

// FormatSerializedValue implements the Custom interface.
func (c customDefinition) FormatSerializedValue(val []byte) (string, error) {
	deserialized, err := c.structure.DeserializeValue(c, val)
	if err != nil {
		return "", err
	}
	return c.structure.FormatValue(c, deserialized)
}

// SerializeType implements the Custom interface.
func (c customDefinition) SerializeType() uint8 {
	return c.id
}

// Compare implements the sql.Type interface.
func (c customDefinition) Compare(v1 any, v2 any) (int, error) {
	return cFuncsCompare[c.functions.Compare](c, v1, v2)
}

// Convert implements the sql.Type interface.
func (c customDefinition) Convert(val any) (any, sql.ConvertInRange, error) {
	return cFuncsConvert[c.functions.Convert](c, val)
}

// Equals implements the sql.Type interface.
func (c customDefinition) Equals(otherType sql.Type) bool {
	return cFuncsEquals[c.functions.Equals](c, otherType)
}

// MaxTextResponseByteLength implements the sql.Type interface.
func (c customDefinition) MaxTextResponseByteLength(ctx *sql.Context) uint32 {
	return cFuncsMaxTextResponseByteLength[c.functions.MaxTextResponseByteLength](ctx, c)
}

// Promote implements the sql.Type interface.
func (c customDefinition) Promote() sql.Type {
	return c.WithStructure(cFuncsPromote[c.functions.Promote](c))
}

// SQL implements the sql.Type interface.
func (c customDefinition) SQL(ctx *sql.Context, dest []byte, val any) (sqltypes.Value, error) {
	return cFuncsSQL[c.functions.SQL](ctx, c, dest, val)
}

// Type implements the sql.Type interface.
func (c customDefinition) Type() query.Type {
	return cFuncsType[c.functions.Type](c)
}

// ValueType implements the sql.Type interface.
func (c customDefinition) ValueType() reflect.Type {
	return cFuncsValueType[c.functions.ValueType](c)
}

// Zero implements the sql.Type interface.
func (c customDefinition) Zero() any {
	return cFuncsZero[c.functions.Zero](c)
}

// CollationCoercibility implements the sql.Type interface.
func (c customDefinition) CollationCoercibility(*sql.Context) (sql.CollationID, byte) {
	return sql.Collation_binary, 5
}

// String implements the sql.Type interface.
func (c customDefinition) String() string {
	return cFuncsString[c.functions.String](c)
}
