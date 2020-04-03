package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCatalogLockTable(t *testing.T) {
	require := require.New(t)
	c := NewCatalog()

	ctx1 := NewContext(context.Background())
	ctx1.SetCurrentDatabase("db1")
	ctx2 := NewContext(context.Background())
	ctx2.SetCurrentDatabase("db1")

	c.LockTable(ctx1, "foo")
	c.LockTable(ctx2, "bar")
	c.LockTable(ctx1, "baz")
	ctx1.SetCurrentDatabase("db2")
	c.LockTable(ctx1, "qux")

	expected := sessionLocks{
		ctx1.ID(): dbLocks{
			"db1": tableLocks{
				"foo": struct{}{},
				"baz": struct{}{},
			},
			"db2": tableLocks{
				"qux": struct{}{},
			},
		},
		ctx2.ID(): dbLocks{
			"db1": tableLocks{
				"bar": struct{}{},
			},
		},
	}

	require.Equal(expected, c.locks)
}
