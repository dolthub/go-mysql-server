// Copyright 2023 Dolthub, Inc.
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

package json

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"
	"testing"
)

func TestInsert(t *testing.T) {
	_, err := NewJSONInsert()
	require.True(t, errors.Is(err, sql.ErrInvalidArgumentNumber))

	for _, format := range jsonFormatTests {
		t.Run(format.name, func(t *testing.T) {
			testCases := JsonInsertTestCases(t, format.prepareFunc)
			RunJsonTests(t, testCases)
		})
	}
}
