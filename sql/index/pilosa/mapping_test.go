// +build !windows

package pilosa

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRowID(t *testing.T) {
	require := require.New(t)
	setup(t)
	defer cleanup(t)
	m := newMapping(filepath.Join(tmpDir, "id.map"))
	require.NoError(m.open())
	defer m.close()

	cases := []int{0, 1, 2, 3, 4, 5, 5, 0, 3, 2, 1, 5}
	expected := []uint64{1, 2, 3, 4, 5, 6, 6, 1, 4, 3, 2, 6}

	for i, c := range cases {
		rowID, err := m.getRowID("frame name", c)
		require.NoError(err)
		require.Equal(expected[i], rowID)
	}

	maxRowID, err := m.getMaxRowID("frame name")
	require.NoError(err)
	require.Equal(uint64(6), maxRowID)
}

func TestLocation(t *testing.T) {
	require := require.New(t)
	setup(t)
	defer cleanup(t)

	m := newMapping(filepath.Join(tmpDir, "id.map"))
	require.NoError(m.open())
	defer m.close()

	cases := map[uint64]string{
		0: "zero",
		1: "one",
		2: "two",
		3: "three",
		4: "four",
	}

	for colID, loc := range cases {
		err := m.putLocation("index name", colID, []byte(loc))
		require.NoError(err)
	}

	for colID, loc := range cases {
		b, err := m.getLocation("index name", colID)
		require.NoError(err)
		require.Equal(loc, string(b))
	}
}

func TestGet(t *testing.T) {
	require := require.New(t)
	setup(t)
	defer cleanup(t)

	m := newMapping(filepath.Join(tmpDir, "id.map"))
	require.NoError(m.open())
	defer m.close()

	cases := []int{0, 1, 2, 3, 4, 5, 5, 0, 3, 2, 1, 5}
	expected := []uint64{1, 2, 3, 4, 5, 6, 6, 1, 4, 3, 2, 6}

	for i, c := range cases {
		m.getRowID("frame name", c)

		id, err := m.get("frame name", c)
		val := binary.LittleEndian.Uint64(id)

		require.NoError(err)
		require.Equal(expected[i], val)
	}
}

type mockPartition string

func (m mockPartition) Key() []byte {
	return []byte(m)
}
