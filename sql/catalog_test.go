package sql_test

import (
	"testing"

	"github.com/gitql/gitql/mem"
	"github.com/gitql/gitql/sql"

	"github.com/gitql/gitql/sql/expression"
	"github.com/stretchr/testify/assert"
)

func TestCatalog_Database(t *testing.T) {
	assert := assert.New(t)

	c := sql.NewCatalog()
	db, err := c.Database("foo")
	assert.EqualError(err, "database not found: foo")
	assert.Nil(db)

	mydb := mem.NewDatabase("foo")
	c.Databases = append(c.Databases, mydb)

	db, err = c.Database("foo")
	assert.NoError(err)
	assert.Equal(mydb, db)
}

func TestCatalog_Table(t *testing.T) {
	assert := assert.New(t)

	c := sql.NewCatalog()

	table, err := c.Table("foo", "bar")
	assert.EqualError(err, "database not found: foo")
	assert.Nil(table)

	mydb := mem.NewDatabase("foo")
	c.Databases = append(c.Databases, mydb)

	table, err = c.Table("foo", "bar")
	assert.EqualError(err, "table not found: bar")
	assert.Nil(table)

	mytable := mem.NewTable("bar", sql.Schema{})
	mydb.AddTable("bar", mytable)

	table, err = c.Table("foo", "bar")
	assert.NoError(err)
	assert.Equal(mytable, table)
}

func TestCatalog_RegisterFunction_NoArgs(t *testing.T) {
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

func TestCatalog_RegisterFunction_OneArg(t *testing.T) {
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

func TestCatalog_RegisterFunction_Variadic(t *testing.T) {
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

func TestCatalog_RegisterFunction_OneAndVariadic(t *testing.T) {
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

func TestCatalog_RegisterFunction_Invalid(t *testing.T) {
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

func TestCatalog_Function_NotExists(t *testing.T) {
	assert := assert.New(t)

	c := sql.NewCatalog()
	f, err := c.Function("func")
	assert.NotNil(err)
	assert.Nil(f)
}
