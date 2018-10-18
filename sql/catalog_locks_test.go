package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCatalogLockTable(t *testing.T) {
	require := require.New(t)
	c := NewCatalog()
	c.SetCurrentDatabase("db1")
	c.LockTable(1, "foo")
	c.LockTable(2, "bar")
	c.LockTable(1, "baz")
	c.SetCurrentDatabase("db2")
	c.LockTable(1, "qux")

	expected := sessionLocks{
		1: dbLocks{
			"db1": tableLocks{
				"foo": struct{}{},
				"baz": struct{}{},
			},
			"db2": tableLocks{
				"qux": struct{}{},
			},
		},
		2: dbLocks{
			"db1": tableLocks{
				"bar": struct{}{},
			},
		},
	}

	require.Equal(expected, c.locks)
}
