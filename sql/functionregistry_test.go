package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestFunctionRegistry(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()
	name := "func"
	var expected sql.Expression = expression.NewStar()
	c.RegisterFunction(name, sql.Function1(func(arg sql.Expression) sql.Expression {
		return expected
	}))

	f, err := c.Function(name)
	require.NoError(err)

	e, err := f.Call()
	require.Error(err)
	require.Nil(e)

	e, err = f.Call(expression.NewStar())
	require.NoError(err)
	require.Equal(expected, e)

	e, err = f.Call(expression.NewStar(), expression.NewStar())
	require.Error(err)
	require.Nil(e)
}

func TestFunctionRegistryMissingFunction(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()
	f, err := c.Function("func")
	require.Error(err)
	require.Nil(f)
}
