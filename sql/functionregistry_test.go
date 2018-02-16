package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestFunctionRegistry_RegisterFunction_NoArgs(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()
	name := "func"
	var expected sql.Expression = expression.NewStar()
	err := c.RegisterFunction(name, func() sql.Expression {
		return expected
	})
	require.Nil(err)

	f, err := c.Function(name)
	require.Nil(err)

	e, err := f.Build()
	require.Nil(err)
	require.Equal(expected, e)

	e, err = f.Build(expression.NewStar())
	require.NotNil(err)
	require.Nil(e)

	e, err = f.Build(expression.NewStar(), expression.NewStar())
	require.NotNil(err)
	require.Nil(e)
}

func TestFunctionRegistry_RegisterFunction_OneArg(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()
	name := "func"
	var expected sql.Expression = expression.NewStar()
	err := c.RegisterFunction(name, func(sql.Expression) sql.Expression {
		return expected
	})
	require.Nil(err)

	f, err := c.Function(name)
	require.Nil(err)

	e, err := f.Build()
	require.NotNil(err)
	require.Nil(e)

	e, err = f.Build(expression.NewStar())
	require.Nil(err)
	require.Equal(expected, e)

	e, err = f.Build(expression.NewStar(), expression.NewStar())
	require.NotNil(err)
	require.Nil(e)
}

func TestFunctionRegistry_RegisterFunction_Variadic(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()
	name := "func"
	var expected sql.Expression = expression.NewStar()
	err := c.RegisterFunction(name, func(...sql.Expression) sql.Expression {
		return expected
	})
	require.Nil(err)

	f, err := c.Function(name)
	require.Nil(err)

	e, err := f.Build()
	require.Nil(err)
	require.Equal(expected, e)

	e, err = f.Build(expression.NewStar())
	require.Nil(err)
	require.Equal(expected, e)

	e, err = f.Build(expression.NewStar(), expression.NewStar())
	require.Nil(err)
	require.Equal(expected, e)
}

func TestFunctionRegistry_RegisterFunction_OneAndVariadic(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()
	name := "func"
	var expected sql.Expression = expression.NewStar()
	err := c.RegisterFunction(name, func(sql.Expression, ...sql.Expression) sql.Expression {
		return expected
	})
	require.Nil(err)

	f, err := c.Function(name)
	require.Nil(err)

	e, err := f.Build()
	require.NotNil(err)
	require.Nil(e)

	e, err = f.Build(expression.NewStar())
	require.Nil(err)
	require.Equal(expected, e)

	e, err = f.Build(expression.NewStar(), expression.NewStar())
	require.Nil(err)
	require.Equal(expected, e)
}

func TestFunctionRegistry_RegisterFunction_Invalid(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()
	name := "func"
	err := c.RegisterFunction(name, func(sql.Table) sql.Expression {
		return nil
	})
	require.NotNil(err)

	err = c.RegisterFunction(name, func(sql.Expression) sql.Table {
		return nil
	})
	require.NotNil(err)

	err = c.RegisterFunction(name, func(sql.Expression) (sql.Table, error) {
		return nil, nil
	})
	require.NotNil(err)

	err = c.RegisterFunction(name, 1)
	require.NotNil(err)
}

func TestFunctionRegistry_Function_NotExist(t *testing.T) {
	require := require.New(t)

	c := sql.NewCatalog()
	f, err := c.Function("func")
	require.NotNil(err)
	require.Nil(f)
}
