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

package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const expectedTree = `Project(a, b)
 ├─ CrossJoin
 │   ├─ TableA
 │   └─ TableB
 └─ CrossJoin
     ├─ TableC
     └─ TableD
`

func TestTreePrinter(t *testing.T) {
	p := NewTreePrinter()
	p.WriteNode("Project(%s, %s)", "a", "b")

	p2 := NewTreePrinter()
	p2.WriteNode("CrossJoin")
	p2.WriteChildren(
		"TableA",
		"TableB",
	)

	p3 := NewTreePrinter()
	p3.WriteNode("CrossJoin")
	p3.WriteChildren(
		"TableC",
		"TableD",
	)

	p.WriteChildren(
		p2.String(),
		p3.String(),
	)

	require.Equal(t, expectedTree, p.String())
}
