package sql_test

import (
	"testing"

	"github.com/gitql/gitql/sql"
	"github.com/gitql/gitql/sql/expression"

	"github.com/stretchr/testify/assert"
)

func TestFunctionRegistry_RegisterFunction_NoArgs(t *testing.T) {
	assert := assert.New(t)

	c := sql.NewCatalog()
	name := "func"
	var expected sql.Expression = expression.NewStar()
	err := c.RegisterFunction(name, func() sql.Expression {
		return expected
	})
	assert.Nil(err)

	f, err := c.Function(name)
	assert.Nil(err)

	e, err := f.Build()
	assert.Nil(err)
	assert.Equal(expected, e)

	e, err = f.Build(expression.NewStar())
	assert.NotNil(err)
	assert.Nil(e)

	e, err = f.Build(expression.NewStar(), expression.NewStar())
	assert.NotNil(err)
	assert.Nil(e)
}

func TestFunctionRegistry_RegisterFunction_OneArg(t *testing.T) {
	assert := assert.New(t)

	c := sql.NewCatalog()
	name := "func"
	var expected sql.Expression = expression.NewStar()
	err := c.RegisterFunction(name, func(sql.Expression) sql.Expression {
		return expected
	})
	assert.Nil(err)

	f, err := c.Function(name)
	assert.Nil(err)

	e, err := f.Build()
	assert.NotNil(err)
	assert.Nil(e)

	e, err = f.Build(expression.NewStar())
	assert.Nil(err)
	assert.Equal(expected, e)

	e, err = f.Build(expression.NewStar(), expression.NewStar())
	assert.NotNil(err)
	assert.Nil(e)
}

func TestFunctionRegistry_RegisterFunction_Variadic(t *testing.T) {
	assert := assert.New(t)

	c := sql.NewCatalog()
	name := "func"
	var expected sql.Expression = expression.NewStar()
	err := c.RegisterFunction(name, func(...sql.Expression) sql.Expression {
		return expected
	})
	assert.Nil(err)

	f, err := c.Function(name)
	assert.Nil(err)

	e, err := f.Build()
	assert.Nil(err)
	assert.Equal(expected, e)

	e, err = f.Build(expression.NewStar())
	assert.Nil(err)
	assert.Equal(expected, e)

	e, err = f.Build(expression.NewStar(), expression.NewStar())
	assert.Nil(err)
	assert.Equal(expected, e)
}

func TestFunctionRegistry_RegisterFunction_OneAndVariadic(t *testing.T) {
	assert := assert.New(t)

	c := sql.NewCatalog()
	name := "func"
	var expected sql.Expression = expression.NewStar()
	err := c.RegisterFunction(name, func(sql.Expression, ...sql.Expression) sql.Expression {
		return expected
	})
	assert.Nil(err)

	f, err := c.Function(name)
	assert.Nil(err)

	e, err := f.Build()
	assert.NotNil(err)
	assert.Nil(e)

	e, err = f.Build(expression.NewStar())
	assert.Nil(err)
	assert.Equal(expected, e)

	e, err = f.Build(expression.NewStar(), expression.NewStar())
	assert.Nil(err)
	assert.Equal(expected, e)
}

func TestFunctionRegistry_RegisterFunction_Invalid(t *testing.T) {
	assert := assert.New(t)

	c := sql.NewCatalog()
	name := "func"
	err := c.RegisterFunction(name, func(sql.Table) sql.Expression {
		return nil
	})
	assert.NotNil(err)

	err = c.RegisterFunction(name, func(sql.Expression) sql.Table {
		return nil
	})
	assert.NotNil(err)

	err = c.RegisterFunction(name, func(sql.Expression) (sql.Table, error) {
		return nil, nil
	})
	assert.NotNil(err)

	err = c.RegisterFunction(name, 1)
	assert.NotNil(err)
}

func TestFunctionRegistry_Function_NotExist(t *testing.T) {
	assert := assert.New(t)

	c := sql.NewCatalog()
	f, err := c.Function("func")
	assert.NotNil(err)
	assert.Nil(f)
}
