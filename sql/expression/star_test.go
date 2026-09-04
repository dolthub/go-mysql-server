// Copyright 2026 Dolthub, Inc.
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

package expression

import (
	"testing"

	"github.com/dolthub/go-mysql-server/internal/exprtest"
	"github.com/stretchr/testify/require"
)

func TestStarString(t *testing.T) {
	tests := []struct {
		expr     *Star
		expected string
	}{
		{NewStar(), "*"},
		{NewQualifiedStar("normal_name"), "normal_name.*"},
		{NewQualifiedStar("table name"), "`table name`.*"},
	}

	for _, test := range tests {
		require.Equal(t, test.expected, test.expr.String())
		exprtest.AssertStringRoundTrip(t, test.expr.String())
	}
}
