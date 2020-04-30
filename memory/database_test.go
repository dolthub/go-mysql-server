package memory

import (
	"testing"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
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

	err := db.CreateTable(sql.NewEmptyContext(), "test_table", nil)
	require.NoError(err)

	tables = db.Tables()
	require.Equal(1, len(tables))
	tt, ok := tables["test_table"]
	require.True(ok)
	require.NotNil(tt)

	err = db.CreateTable(sql.NewEmptyContext(), "test_table", nil)
	require.Error(err)
}
