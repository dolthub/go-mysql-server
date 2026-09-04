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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/internal/exprtest"
	"github.com/dolthub/go-mysql-server/sql"
)

func TestSystemVarString(t *testing.T) {
	tests := []struct {
		scope    string
		expected string
	}{
		{"", "@@unregistered_system_variable"},
		{"session", "@@session.unregistered_system_variable"},
	}
	for _, tt := range tests {
		expr := NewSystemVar("unregistered_system_variable", sql.GetMysqlScope(sql.SystemVariableScope_Session), tt.scope)
		require.Equal(t, tt.expected, expr.String())
		parsed := exprtest.RequireColumn(t, exprtest.ParseExpression(t, expr))
		parts := strings.Split(strings.TrimPrefix(parsed.Name.String(), "@@"), ".")
		if expr.SpecifiedScope == "" {
			require.Equal(t, []string{expr.Name}, parts)
		} else {
			require.Equal(t, []string{expr.SpecifiedScope, expr.Name}, parts)
		}
	}
}

func TestUserVarString(t *testing.T) {
	tests := []struct {
		name     string
		expected string
	}{
		{"normal_name", "@normal_name"},
		{"var name", "@`var name`"},
		{"select", "@`select`"},
	}

	for _, test := range tests {
		expr := NewUserVar(test.name)
		require.Equal(t, test.expected, expr.String())
		parsed := exprtest.RequireColumn(t, exprtest.ParseExpression(t, expr))
		parsedName := strings.TrimPrefix(parsed.Name.String(), "@")
		if strings.HasPrefix(parsedName, "`") {
			parsedName = strings.ReplaceAll(strings.Trim(parsedName, "`"), "``", "`")
		}
		require.Equal(t, expr.Name, parsedName)
	}
}
