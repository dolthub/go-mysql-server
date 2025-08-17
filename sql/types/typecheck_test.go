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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestIsGeometry(t *testing.T) {
	assert.True(t, IsGeometry(GeometryType{}))
	assert.True(t, IsGeometry(PointType{}))
	assert.True(t, IsGeometry(LineStringType{}))
	assert.True(t, IsGeometry(PolygonType{}))
	assert.False(t, IsGeometry(StringType{}))
	assert.False(t, IsGeometry(JSON))
	assert.False(t, IsGeometry(Blob))
}

func TestIsJSON(t *testing.T) {
	assert.True(t, IsJSON(JSON))
	assert.False(t, IsJSON(Blob))
	assert.False(t, IsJSON(NumberTypeImpl_{}))
	assert.False(t, IsJSON(StringType{}))
}

func TestSystemTypesIsNumber(t *testing.T) {
	assert.True(t, IsNumber(SystemBoolType{}))
	assert.True(t, IsNumber(systemIntType{}))
	assert.True(t, IsNumber(systemUintType{}))
	assert.True(t, IsNumber(systemDoubleType{}))
	assert.False(t, IsNumber(systemEnumType{}))
	assert.False(t, IsNumber(systemSetType{}))
	assert.False(t, IsNumber(systemStringType{}))
}

func TestSystemTypesIsSigned(t *testing.T) {
	assert.True(t, IsSigned(SystemBoolType{}))
	assert.True(t, IsSigned(systemIntType{}))
	assert.False(t, IsSigned(systemUintType{}))
	assert.False(t, IsSigned(systemDoubleType{}))
	assert.False(t, IsSigned(systemEnumType{}))
	assert.False(t, IsSigned(systemSetType{}))
	assert.False(t, IsSigned(systemStringType{}))
}

func TestSystemTypesIsUnsigned(t *testing.T) {
	assert.False(t, IsUnsigned(SystemBoolType{}))
	assert.False(t, IsUnsigned(systemIntType{}))
	assert.True(t, IsUnsigned(systemUintType{}))
	assert.False(t, IsUnsigned(systemDoubleType{}))
	assert.False(t, IsUnsigned(systemEnumType{}))
	assert.False(t, IsUnsigned(systemSetType{}))
	assert.False(t, IsUnsigned(systemStringType{}))
}

func TestSystemTypesIsText(t *testing.T) {
	assert.False(t, IsText(SystemBoolType{}))
	assert.False(t, IsText(systemIntType{}))
	assert.False(t, IsText(systemUintType{}))
	assert.False(t, IsText(systemDoubleType{}))
	assert.False(t, IsText(systemEnumType{}))
	assert.False(t, IsText(systemSetType{}))
	assert.True(t, IsText(systemStringType{}))
}

func TestSystemTypesIsEnum(t *testing.T) {
	assert.False(t, IsEnum(SystemBoolType{}))
	assert.False(t, IsEnum(systemIntType{}))
	assert.False(t, IsEnum(systemUintType{}))
	assert.False(t, IsEnum(systemDoubleType{}))
	assert.True(t, IsEnum(systemEnumType{}))
	assert.False(t, IsEnum(systemSetType{}))
	assert.False(t, IsEnum(systemStringType{}))
}

func TestSystemTypesIsSet(t *testing.T) {
	assert.False(t, IsSet(SystemBoolType{}))
	assert.False(t, IsSet(systemIntType{}))
	assert.False(t, IsSet(systemUintType{}))
	assert.False(t, IsSet(systemDoubleType{}))
	assert.False(t, IsSet(systemEnumType{}))
	assert.True(t, IsSet(NewSystemSetType("", sql.Collation_Default, "")))
	assert.False(t, IsSet(systemStringType{}))
}
