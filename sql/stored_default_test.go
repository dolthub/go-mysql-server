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

package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// init registers the repair that TestRepairStoredDefault relies on.
func init() {
	RegisterStoredDefaultRewrite("repair_test", func(text string) string {
		return strings.ReplaceAll(text, "'x'", "x")
	})
}

func TestRepairStoredDefault(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/12340
	t.Run("registered call is repaired", func(t *testing.T) {
		require.Equal(t, "(repair_test(x))", RepairStoredDefault("(repair_test('x'))"))
	})
	t.Run("unregistered call is left alone", func(t *testing.T) {
		require.Equal(t, "(other('x'))", RepairStoredDefault("(other('x'))"))
	})
	t.Run("name inside a string literal is not a call", func(t *testing.T) {
		text := "(concat('repair_test(', 'x'))"
		require.Equal(t, text, RepairStoredDefault(text))
	})
}

func TestRegisterStoredDefaultRewrite(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/12340
	t.Run("duplicate name panics", func(t *testing.T) {
		require.Panics(t, func() {
			RegisterStoredDefaultRewrite("repair_test", func(text string) string { return text })
		})
	})
}
