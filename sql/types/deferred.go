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

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/sql"
)

type deferredType struct {
	bindVar string
}

var _ sql.DeferredType = (*deferredType)(nil)

func NewDeferredType(name string) sql.Type {
	return &deferredType{bindVar: name}
}

func (t deferredType) Equals(otherType sql.Type) bool {
	return false
}

// Compare implements Type interface. Note that while this returns 0 (equals)
// for ordering purposes, in SQL NULL != NULL.
func (t deferredType) Compare(a interface{}, b interface{}) (int, error) {
	return 0, nil
}

// Convert implements Type interface.
func (t deferredType) Convert(v interface{}) (interface{}, error) {
	if v != nil {
		return nil, ErrValueNotNil.New(v)
	}

	return nil, nil
}

// MaxTextResponseByteLength implements the Type interface
func (t deferredType) MaxTextResponseByteLength() uint32 {
	// deferredType is never actually sent over the wire
	return 0
}

// MustConvert implements the Type interface.
func (t deferredType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Promote implements the Type interface.
func (t deferredType) Promote() sql.Type {
	return t
}

// SQL implements Type interface.
func (t deferredType) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	return sqltypes.NULL, nil
}

// String implements Type interface.
func (t deferredType) String() string {
	return "deferred"
}

// Type implements Type interface.
func (t deferredType) Type() query.Type {
	return sqltypes.Expression
}

// ValueType implements Type interface.
func (t deferredType) ValueType() reflect.Type {
	return nil
}

// Zero implements Type interface.
func (t deferredType) Zero() interface{} {
	return nil
}

func (t deferredType) IsDeferred() bool {
	return true
}

func (t deferredType) Name() string {
	return t.bindVar
}

func IsDeferredType(t sql.Type) bool {
	_, ok := t.(sql.DeferredType)
	return ok
}
