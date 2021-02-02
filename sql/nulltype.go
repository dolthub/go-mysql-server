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

package sql

import (
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	Null NullType = nullType{}

	// ErrValueNotNil is thrown when a value that was expected to be nil, is not
	ErrValueNotNil = errors.NewKind("value not nil: %#v")
)

type NullType interface {
	Type
}

type nullType struct{}

// Compare implements Type interface. Note that while this returns 0 (equals)
// for ordering purposes, in SQL NULL != NULL.
func (t nullType) Compare(a interface{}, b interface{}) (int, error) {
	return 0, nil
}

// Convert implements Type interface.
func (t nullType) Convert(v interface{}) (interface{}, error) {
	if v != nil {
		return nil, ErrValueNotNil.New(v)
	}

	return nil, nil
}

// MustConvert implements the Type interface.
func (t nullType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Promote implements the Type interface.
func (t nullType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t nullType) SQL(interface{}) (sqltypes.Value, error) {
	return sqltypes.NULL, nil
}

// String implements Type interface.
func (t nullType) String() string {
	return "NULL"
}

// Type implements Type interface.
func (t nullType) Type() query.Type {
	return sqltypes.Null
}

// Zero implements Type interface.
func (t nullType) Zero() interface{} {
	return nil
}
