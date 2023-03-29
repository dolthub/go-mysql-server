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
