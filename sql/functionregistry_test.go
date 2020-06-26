package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

func TestFunctionRegistry(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()
	name := "func"
	var expected sql.Expression = expression.NewStar()
	c.MustRegister(sql.Function1{
		Name: name,
		Fn:   func(arg sql.Expression) sql.Expression { return expected },
	})

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
