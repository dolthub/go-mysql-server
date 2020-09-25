package expression

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
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

	floatF := NewGetField(0, sql.Float64, "col1", true)
	e = NewIsTrue(floatF)
	require.Equal(sql.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(true, eval(t, e, sql.NewRow(1.5)))
	require.Equal(true, eval(t, e, sql.NewRow(-1.5)))
	require.Equal(false, eval(t, e, sql.NewRow(0)))

	stringF := NewGetField(0, sql.Text, "col1", true)
	e = NewIsTrue(stringF)
	require.Equal(sql.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(false, eval(t, e, sql.NewRow("")))
	require.Equal(false, eval(t, e, sql.NewRow("false")))
	require.Equal(false, eval(t, e, sql.NewRow("true")))
}

func TestIsFalse(t *testing.T) {
	require := require.New(t)

	boolF := NewGetField(0, sql.Boolean, "col1", true)
	e := NewIsFalse(boolF)
	require.Equal(sql.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(false, eval(t, e, sql.NewRow(true)))
	require.Equal(true, eval(t, e, sql.NewRow(false)))

	intF := NewGetField(0, sql.Int64, "col1", true)
	e = NewIsFalse(intF)
	require.Equal(sql.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(false, eval(t, e, sql.NewRow(100)))
	require.Equal(false, eval(t, e, sql.NewRow(-1)))
	require.Equal(true, eval(t, e, sql.NewRow(0)))

	floatF := NewGetField(0, sql.Float64, "col1", true)
	e = NewIsFalse(floatF)
	require.Equal(sql.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(false, eval(t, e, sql.NewRow(1.5)))
	require.Equal(false, eval(t, e, sql.NewRow(-1.5)))
	require.Equal(true, eval(t, e, sql.NewRow(0)))

	stringF := NewGetField(0, sql.Text, "col1", true)
	e = NewIsFalse(stringF)
	require.Equal(sql.Boolean, e.Type())
	require.False(e.IsNullable())
	require.Equal(false, eval(t, e, sql.NewRow(nil)))
	require.Equal(true, eval(t, e, sql.NewRow("")))
	require.Equal(true, eval(t, e, sql.NewRow("false")))
	require.Equal(true, eval(t, e, sql.NewRow("true")))
}
