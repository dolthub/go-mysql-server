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

package exprtest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAssertStringRoundTrip(t *testing.T) {
	AssertStringRoundTrip(t, "SUM(`value`) over (partition by bucket)")
	AssertStringRoundTrip(t, "42 AS `alias name`")
	AssertStringRoundTrip(t, "group_concat(value separator 'a''b\\\\c')")
}

func TestNormalizePreservesStringValues(t *testing.T) {
	require.NotEqual(t, normalize("concat('a b')"), normalize("concat('ab')"))
	require.NotEqual(t, normalize("concat('ABC')"), normalize("concat('abc')"))
}
