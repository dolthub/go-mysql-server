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

package similartext

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFind(t *testing.T) {
	require := require.New(t)

	var names []string
	res := Find(names, "")
	require.Empty(res)

	names = []string{"foo", "bar", "aka", "ake"}
	res = Find(names, "baz")
	require.Equal(", maybe you mean bar?", res)

	res = Find(names, "")
	require.Empty(res)

	res = Find(names, "foo")
	require.Equal(", maybe you mean foo?", res)

	res = Find(names, "willBeTooDifferent")
	require.Empty(res)

	res = Find(names, "aki")
	require.Equal(", maybe you mean aka or ake?", res)
}

func TestFindFromMap(t *testing.T) {
	require := require.New(t)

	var names map[string]int
	res := FindFromMap(names, "")
	require.Empty(res)

	names = map[string]int{
		"foo": 1,
		"bar": 2,
	}
	res = FindFromMap(names, "baz")
	require.Equal(", maybe you mean bar?", res)

	res = FindFromMap(names, "")
	require.Empty(res)

	res = FindFromMap(names, "foo")
	require.Equal(", maybe you mean foo?", res)
}
