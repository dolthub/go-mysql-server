package plan

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/mem"
	"gopkg.in/sqle/sqle.v0/sql"
	"gopkg.in/sqle/sqle.v0/sql/expression"

	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	assert := assert.New(t)
	childSchema := sql.Schema{
		{Name: "col1", Type: sql.String, Nullable: true},
		{Name: "col2", Type: sql.String, Nullable: true},
		{Name: "col3", Type: sql.Integer, Nullable: true},
		{Name: "col4", Type: sql.BigInteger, Nullable: true},
	}
	child := mem.NewTable("test", childSchema)
	err := child.Insert(sql.NewRow("col1_1", "col2_1", int32(1111), int64(2222)))
	assert.Nil(err)
	err = child.Insert(sql.NewRow("col1_2", "col2_2", int32(3333), int64(4444)))
	assert.Nil(err)
	err = child.Insert(sql.NewRow("col1_3", "col2_3", nil, int64(4444)))
	assert.Nil(err)

	f := NewFilter(
		expression.NewEquals(
			expression.NewGetField(0, sql.String, "col1", true),
			expression.NewLiteral("col1_1", sql.String)),
		child)

	assert.Equal(1, len(f.Children()))

	iter, err := f.RowIter()
	assert.Nil(err)
	assert.NotNil(iter)

	row, err := iter.Next()
	assert.Nil(err)
	assert.NotNil(row)

	assert.Equal("col1_1", row[0])
	assert.Equal("col2_1", row[1])

	row, err = iter.Next()
	assert.NotNil(err)
	assert.Nil(row)

	f = NewFilter(expression.NewEquals(
		expression.NewGetField(2, sql.Integer, "col3", true),
		expression.NewLiteral(int32(1111),
			sql.Integer)), child)

	iter, err = f.RowIter()
	assert.Nil(err)
	assert.NotNil(iter)

	row, err = iter.Next()
	assert.Nil(err)
	assert.NotNil(row)

	assert.Equal(int32(1111), row[2])
	assert.Equal(int64(2222), row[3])

	f = NewFilter(expression.NewEquals(
		expression.NewGetField(3, sql.BigInteger, "col4", true),
		expression.NewLiteral(int64(4444), sql.BigInteger)),
		child)

	iter, err = f.RowIter()
	assert.Nil(err)
	assert.NotNil(iter)

	row, err = iter.Next()
	assert.Nil(err)
	assert.NotNil(row)

	assert.Equal(int32(3333), row[2])
	assert.Equal(int64(4444), row[3])
}
