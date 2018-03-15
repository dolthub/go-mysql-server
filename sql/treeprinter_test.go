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
