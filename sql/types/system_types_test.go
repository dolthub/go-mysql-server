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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestSystemTypesImplementSqlTypeInterfaces(t *testing.T) {
	assert.True(t, sql.IsNumberType(SystemBoolType{}))
	assert.True(t, sql.IsNumberType(systemIntType{}))
	assert.True(t, sql.IsNumberType(systemUintType{}))
	assert.True(t, sql.IsNumberType(systemDoubleType{}))

	assert.False(t, sql.IsNumberType(systemEnumType{}))
	assert.False(t, sql.IsNumberType(systemSetType{}))
	assert.False(t, sql.IsNumberType(systemStringType{}))

	assert.True(t, sql.IsStringType(systemStringType{}))
	assert.False(t, sql.IsStringType(SystemBoolType{}))
}
