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
	"fmt"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

type AutoIncrementType interface {
	Type
	CurrentPlaceholder() interface{}
}

type autoIncrementType struct {
	placeholder interface{}
	underlying Type
}

var _ AutoIncrementType = (*autoIncrementType)(nil)

func CreateAutoIncrementType(currentValue interface{}, underlying Type) AutoIncrementType {
	return autoIncrementType{
		placeholder: currentValue,
		underlying: underlying,
	}
}

func (a autoIncrementType) Compare(i interface{}, i2 interface{}) (int, error) {
	return a.underlying.Compare(i, i2)
}

func (a autoIncrementType) Convert(i interface{}) (interface{}, error) {
	return a.underlying.Convert(i)
}

func (a autoIncrementType) Promote() Type {
	return a.underlying.Promote()
}

func (a autoIncrementType) SQL(i interface{}) (sqltypes.Value, error) {
	if i == nil {
		return sqltypes.NULL, nil
	}
	value, err := a.Convert(i)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return sqltypes.NewUint64(value.(uint64)), nil
}

func (a autoIncrementType) Type() query.Type {
	return sqltypes.Uint64
}

func (a autoIncrementType) Zero() interface{} {
	return uint64(0)
}

func (a autoIncrementType) String() string {
	return fmt.Sprintf("AUTO_INCREMENT({})")
}

func (a autoIncrementType) CurrentPlaceholder() interface{} {
	return a.placeholder
}