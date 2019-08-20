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
