package pilosa

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pilosa/pilosa"
	"github.com/stretchr/testify/require"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestCompare(t *testing.T) {
	now := time.Now()
	testCases := []struct {
		a, b     interface{}
		err      *errors.Kind
		expected int
	}{
		{true, true, nil, 0},
		{false, true, nil, -1},
		{true, false, nil, 1},
		{false, false, nil, 0},
		{true, 0, errTypeMismatch, 0},

		{"a", "b", nil, -1},
		{"b", "a", nil, 1},
		{"a", "a", nil, 0},
		{"a", 1, errTypeMismatch, 0},

		{int32(1), int32(2), nil, -1},
		{int32(2), int32(1), nil, 1},
		{int32(2), int32(2), nil, 0},
		{int32(1), "", errTypeMismatch, 0},

		{int64(1), int64(2), nil, -1},
		{int64(2), int64(1), nil, 1},
		{int64(2), int64(2), nil, 0},
		{int64(1), "", errTypeMismatch, 0},

		{uint32(1), uint32(2), nil, -1},
		{uint32(2), uint32(1), nil, 1},
		{uint32(2), uint32(2), nil, 0},
		{uint32(1), "", errTypeMismatch, 0},

		{uint64(1), uint64(2), nil, -1},
		{uint64(2), uint64(1), nil, 1},
		{uint64(2), uint64(2), nil, 0},
		{uint64(1), "", errTypeMismatch, 0},

		{float64(1), float64(2), nil, -1},
		{float64(2), float64(1), nil, 1},
		{float64(2), float64(2), nil, 0},
		{float64(1), "", errTypeMismatch, 0},

		{now.Add(-1 * time.Hour), now, nil, -1},
		{now, now.Add(-1 * time.Hour), nil, 1},
		{now, now, nil, 0},
		{now, 1, errTypeMismatch, -1},

		{[]interface{}{"a", "a"}, []interface{}{"a", "b"}, nil, -1},
		{[]interface{}{"a", "b"}, []interface{}{"a", "a"}, nil, 1},
		{[]interface{}{"a", "a"}, []interface{}{"a", "a"}, nil, 0},
		{[]interface{}{"b"}, []interface{}{"a", "b"}, nil, -1},
		{[]interface{}{"b"}, 1, errTypeMismatch, -1},

		{[]byte{0, 1}, []byte{1, 1}, nil, -1},
		{[]byte{1, 1}, []byte{0, 1}, nil, 1},
		{[]byte{1, 1}, []byte{1, 1}, nil, 0},
		{[]byte{1}, []byte{0, 1}, nil, 1},
		{[]byte{0, 1}, 1, errTypeMismatch, -1},

		{time.Duration(0), nil, errUnknownType, -1},
	}

	for _, tt := range testCases {
		name := fmt.Sprintf("(%T)(%v) and (%T)(%v)", tt.a, tt.a, tt.b, tt.b)
		t.Run(name, func(t *testing.T) {
			require := require.New(t)
			cmp, err := compare(tt.a, tt.b)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, cmp)
			}
		})
	}
}

func TestDecodeGob(t *testing.T) {
	testCases := []interface{}{
		"foo",
		int32(1),
		int64(1),
		uint32(1),
		uint64(1),
		float64(1),
		true,
		time.Date(2018, time.August, 1, 1, 1, 1, 1, time.Local),
		[]byte("foo"),
		[]interface{}{1, 3, 3, 7},
	}

	for _, tt := range testCases {
		name := fmt.Sprintf("(%T)(%v)", tt, tt)
		t.Run(name, func(t *testing.T) {
			require := require.New(t)

			var buf bytes.Buffer
			require.NoError(gob.NewEncoder(&buf).Encode(tt))

			result, err := decodeGob(buf.Bytes(), tt)
			require.NoError(err)
			require.Equal(tt, result)
		})
	}
}

func TestMergeable(t *testing.T) {
	require := require.New(t)
	h := pilosa.NewHolder()
	h.Path = os.TempDir()

	i1, err := h.CreateIndexIfNotExists("i1", pilosa.IndexOptions{})
	require.NoError(err)
	i2, err := h.CreateIndexIfNotExists("i2", pilosa.IndexOptions{})
	require.NoError(err)

	testCases := []struct {
		i1       sql.IndexLookup
		i2       sql.IndexLookup
		expected bool
	}{
		{
			i1:       &indexLookup{index: newConcurrentPilosaIndex(i1)},
			i2:       &indexLookup{index: newConcurrentPilosaIndex(i1)},
			expected: true,
		},
		{
			i1:       &indexLookup{index: newConcurrentPilosaIndex(i1)},
			i2:       &indexLookup{index: newConcurrentPilosaIndex(i2)},
			expected: false,
		},
		{
			i1:       &indexLookup{index: newConcurrentPilosaIndex(i1)},
			i2:       &ascendLookup{filteredLookup: &filteredLookup{index: newConcurrentPilosaIndex(i1)}},
			expected: true,
		},
		{
			i1:       &descendLookup{filteredLookup: &filteredLookup{index: newConcurrentPilosaIndex(i1)}},
			i2:       &ascendLookup{filteredLookup: &filteredLookup{index: newConcurrentPilosaIndex(i1)}},
			expected: true,
		},
		{
			i1:       &descendLookup{filteredLookup: &filteredLookup{index: newConcurrentPilosaIndex(i1)}},
			i2:       &indexLookup{index: newConcurrentPilosaIndex(i2)},
			expected: false,
		},
		{
			i1:       &descendLookup{filteredLookup: &filteredLookup{index: newConcurrentPilosaIndex(i1)}},
			i2:       &descendLookup{filteredLookup: &filteredLookup{index: newConcurrentPilosaIndex(i2)}},
			expected: false,
		},
		{
			i1:       &negateLookup{index: newConcurrentPilosaIndex(i1)},
			i2:       &negateLookup{index: newConcurrentPilosaIndex(i1)},
			expected: true,
		},
		{
			i1:       &negateLookup{index: newConcurrentPilosaIndex(i1)},
			i2:       &negateLookup{index: newConcurrentPilosaIndex(i2)},
			expected: false,
		},
		{
			i1:       &negateLookup{index: newConcurrentPilosaIndex(i1)},
			i2:       &indexLookup{index: newConcurrentPilosaIndex(i1)},
			expected: true,
		},
		{
			i1:       &negateLookup{index: newConcurrentPilosaIndex(i1)},
			i2:       &descendLookup{filteredLookup: &filteredLookup{index: newConcurrentPilosaIndex(i1)}},
			expected: true,
		},
		{
			i1:       &negateLookup{index: newConcurrentPilosaIndex(i1)},
			i2:       &ascendLookup{filteredLookup: &filteredLookup{index: newConcurrentPilosaIndex(i1)}},
			expected: true,
		},
	}

	for _, tc := range testCases {
		m1, ok := tc.i1.(sql.Mergeable)
		require.True(ok)

		require.Equal(tc.expected, m1.IsMergeable(tc.i2))
	}
}

func TestIndexes(t *testing.T) {
	testCases := []sql.IndexLookup{
		&indexLookup{id: "foo"},
		&negateLookup{id: "foo"},
		&ascendLookup{filteredLookup: &filteredLookup{id: "foo"}},
		&descendLookup{filteredLookup: &filteredLookup{id: "foo"}},
	}

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("%T", tt), func(t *testing.T) {
			require.Equal(t, []string{"foo"}, tt.Indexes())
		})
	}
}
