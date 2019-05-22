package sql

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArrayGenerator(t *testing.T) {
	require := require.New(t)

	expected := []interface{}{"a", "b", "c"}
	gen := NewArrayGenerator(expected)

	var values []interface{}
	for {
		v, err := gen.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			require.NoError(err)
		}
		values = append(values, v)
	}

	require.Equal(expected, values)
}

func TestToGenerator(t *testing.T) {
	require := require.New(t)

	gen, err := ToGenerator([]interface{}{1, 2, 3})
	require.NoError(err)
	require.Equal(NewArrayGenerator([]interface{}{1, 2, 3}), gen)

	gen, err = ToGenerator(new(fakeGen))
	require.NoError(err)
	require.Equal(new(fakeGen), gen)

	gen, err = ToGenerator(nil)
	require.NoError(err)
	require.Equal(NewArrayGenerator(nil), gen)

	_, err = ToGenerator("foo")
	require.Error(err)
}

type fakeGen struct{}

func (fakeGen) Next() (interface{}, error) { return nil, fmt.Errorf("not implemented") }
func (fakeGen) Close() error               { return nil }
