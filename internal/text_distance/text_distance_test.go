package text_distance

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindSimilarName(t *testing.T) {
	require := require.New(t)

	var names []string
	res := FindSimilarName(names, "")
	require.Empty(res)

	names = []string{"foo", "bar"}
	res = FindSimilarName(names, "baz")
	require.Equal("bar", res)

	res = FindSimilarName(names, "")
	require.Equal(names[0], res)

	res = FindSimilarName(names, "foo")
	require.Equal("foo", res)
}

func TestFindSimilarNameFromMap(t *testing.T) {
	require := require.New(t)

	var names map[string]int
	res := FindSimilarNameFromMap(names, "")
	require.Empty(res)

	names = map[string]int {
		"foo": 1,
		"bar": 2,
	}
	res = FindSimilarNameFromMap(names, "baz")
	require.Equal("bar", res)

	res = FindSimilarNameFromMap(names, "")
	require.NotEmpty(res)

	res = FindSimilarNameFromMap(names, "foo")
	require.Equal("foo", res)
}
