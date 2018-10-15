package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestShowVariables(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewEmptyContext()
	config := ctx.Session.GetAll()
	sv := NewShowVariables(config, "")
	require.True(sv.Resolved())

	it, err := sv.RowIter(ctx)
	require.NoError(err)

	for row, err := it.Next(); err == nil; row, err = it.Next() {
		key := row[0].(string)
		val := row[1]

		t.Logf("key: %s\tval: %v\n", key, val)

		require.Equal(config[key].Value, val)
		delete(config, key)
	}
	if err != io.EOF {
		require.NoError(err)
	}
	require.NoError(it.Close())
	require.Equal(0, len(config))
}

func TestShowVariablesWithLike(t *testing.T) {
	require := require.New(t)

	vars := map[string]sql.TypedValue{
		"int1": {Typ: sql.Int32, Value: 1},
		"int2": {Typ: sql.Int32, Value: 2},
		"txt":  {Typ: sql.Text, Value: "abcdefghijklmnoprstuwxyz"},
	}

	sv := NewShowVariables(vars, "int%")
	require.True(sv.Resolved())

	it, err := sv.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	for row, err := it.Next(); err == nil; row, err = it.Next() {
		key := row[0].(string)
		val := row[1]
		require.Equal(vars[key].Value, val)
		require.Equal(sql.Int32, vars[key].Typ)
		delete(vars, key)
	}
	if err != io.EOF {
		require.NoError(err)
	}
	require.NoError(it.Close())
	require.Equal(1, len(vars))

	_, ok := vars["txt"]
	require.True(ok)
}
