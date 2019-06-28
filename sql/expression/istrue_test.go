package expression

import (
	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIsTrue(t *testing.T) {
	require := require.New(t)

	boolF := NewGetField(0, sql.Boolean, "col1", true)
	e := NewIsTrue(boolF)
	require.Equal(sql.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(true, eval(t, e, sql.NewRow(true)))
	require.Equal(false, eval(t, e, sql.NewRow(false)))

	intF := NewGetField(0, sql.Int64, "col1", true)
	e = NewIsTrue(intF)
	require.Equal(sql.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(true, eval(t, e, sql.NewRow(100)))
	require.Equal(true, eval(t, e, sql.NewRow(-1)))
	require.Equal(false, eval(t, e, sql.NewRow(0)))
}

func TestIsFalse(t *testing.T) {
	require := require.New(t)

	boolF := NewGetField(0, sql.Boolean, "col1", true)
	e := NewIsFalse(boolF)
	require.Equal(sql.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(true, eval(t, e, sql.NewRow(nil)))
	require.Equal(false, eval(t, e, sql.NewRow(true)))
	require.Equal(true, eval(t, e, sql.NewRow(false)))

	intF := NewGetField(0, sql.Int64, "col1", true)
	e = NewIsFalse(intF)
	require.Equal(sql.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(true, eval(t, e, sql.NewRow(nil)))
	require.Equal(false, eval(t, e, sql.NewRow(100)))
	require.Equal(false, eval(t, e, sql.NewRow(-1)))
	require.Equal(true, eval(t, e, sql.NewRow(0)))
}