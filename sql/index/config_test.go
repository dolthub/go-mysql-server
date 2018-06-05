package index

import (
	"crypto/sha1"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestConfig(t *testing.T) {
	require := require.New(t)

	db, table, id := "db_name", "table_name", "index_id"
	path := filepath.Join(os.TempDir(), db, table, id)
	err := os.MkdirAll(path, 0750)

	require.Nil(err)
	defer os.RemoveAll(path)

	h1 := sha1.Sum([]byte("h1"))
	h2 := sha1.Sum([]byte("h2"))
	exh1 := sql.ExpressionHash(h1[:])
	exh2 := sql.ExpressionHash(h2[:])

	cfg1 := NewConfig(
		db,
		table,
		id,
		[]sql.ExpressionHash{exh1, exh2},
		"DriverID",
		map[string]string{
			"port": "10101",
			"host": "localhost",
		},
	)

	err = WriteConfigFile(path, cfg1)
	require.Nil(err)

	cfg2, err := ReadConfigFile(path)
	require.Equal(cfg1.DB, cfg2.DB)
	require.Equal(cfg1.Table, cfg2.Table)
	require.Equal(cfg1.ID, cfg2.ID)
	require.Truef(reflect.DeepEqual(cfg1.Expressions, cfg2.Expressions),
		"Expected: %v\nGot: %v\n", cfg1.Expressions, cfg2.Expressions)
	require.Truef(reflect.DeepEqual(cfg1.Drivers, cfg2.Drivers),
		"Expected %v\nGot: %v\n", cfg1.Drivers, cfg2.Drivers)
	require.Truef(reflect.DeepEqual(cfg1.Driver("DriverID"), cfg2.Driver("DriverID")),
		"Expected %v\nGot: %v\n", cfg1.Driver("DriverID"), cfg2.Driver("DriverID"))
}
