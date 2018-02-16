package mem

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestDatabase_Name(t *testing.T) {
	require := require.New(t)
	db := NewDatabase("test")
	require.Equal("test", db.Name())
}

func TestDatabase_AddTable(t *testing.T) {
	require := require.New(t)
	db := NewDatabase("test")
	tables := db.Tables()
	require.Equal(0, len(tables))
	table := &Table{"test_table", sql.Schema{}, nil}
	db.AddTable("test_table", table)
	tables = db.Tables()
	require.Equal(1, len(tables))
	tt, ok := tables["test_table"]
	require.True(ok)
	require.NotNil(tt)
}
