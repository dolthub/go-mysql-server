package text_distance

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindSimilarNames(t *testing.T) {
	require := require.New(t)

	var names []string
	res := FindSimilarNames(names, "")
	require.Empty(res)

	names = []string{"foo", "bar", "aka", "ake"}
	res = FindSimilarNames(names, "baz")
	require.Equal(", maybe you mean bar?", res)

	res = FindSimilarNames(names, "")
	require.Empty(res)

	res = FindSimilarNames(names, "foo")
	require.Equal(", maybe you mean foo?", res)

	res = FindSimilarNames(names, "willBeTooDifferent")
	require.Empty(res)

	res = FindSimilarNames(names, "aki")
	require.Equal(", maybe you mean aka or ake?", res)
}

func TestFindSimilarNamesFromMap(t *testing.T) {
	require := require.New(t)

	var names map[string]int
	res := FindSimilarNamesFromMap(names, "")
	require.Empty(res)

	names = map[string]int {
		"foo": 1,
		"bar": 2,
	}
	res = FindSimilarNamesFromMap(names, "baz")
	require.Equal(", maybe you mean bar?", res)

	res = FindSimilarNamesFromMap(names, "")
	require.Empty(res)

	res = FindSimilarNamesFromMap(names, "foo")
	require.Equal(", maybe you mean foo?", res)
}
