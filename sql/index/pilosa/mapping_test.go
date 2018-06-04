package pilosa

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRowID(t *testing.T) {
	require := require.New(t)

	path, err := mkdir(os.TempDir(), "mapping_test")
	require.Nil(err)
	defer os.RemoveAll(path)

	m, err := openMapping(path)
	require.Nil(err)
	defer m.close()

	cases := []int{0, 1, 2, 3, 4, 5, 5, 0, 3, 2, 1, 5}
	expected := []uint64{0, 1, 2, 3, 4, 5, 5, 0, 3, 2, 1, 5}

	for i, c := range cases {
		rowID, err := m.getRowID("frame name", c)
		require.Nil(err)
		require.Equal(expected[i], rowID)
	}
}

func TestLocation(t *testing.T) {
	require := require.New(t)

	path, err := mkdir(os.TempDir(), "mapping_test")
	require.Nil(err)
	defer os.RemoveAll(path)

	m, err := openMapping(path)
	require.Nil(err)
	defer m.close()

	cases := map[uint64]string{
		0: "zero",
		1: "one",
		2: "two",
		3: "three",
		4: "four",
	}

	for colID, loc := range cases {
		err = m.putLocation("index name", colID, []byte(loc))
		require.Nil(err)
	}

	for colID, loc := range cases {
		b, err := m.getLocation("index name", colID)
		require.Nil(err)
		require.Equal(loc, string(b))
	}
}

func TestGet(t *testing.T) {
	require := require.New(t)

	path, err := mkdir(os.TempDir(), "mapping_test")
	require.Nil(err)
	defer os.RemoveAll(path)

	m, err := openMapping(path)
	require.Nil(err)
	defer m.close()

	cases := []int{0, 1, 2, 3, 4, 5, 5, 0, 3, 2, 1, 5}
	expected := []uint64{0, 1, 2, 3, 4, 5, 5, 0, 3, 2, 1, 5}

	for i, c := range cases {
		m.getRowID("frame name", c)

		id, err := m.get("frame name", c)
		val := binary.LittleEndian.Uint64(id)

		require.Nil(err)
		require.Equal(expected[i], val)
	}
}
