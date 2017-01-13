package plan

import (
	"testing"

	"github.com/gitql/gitql/mem"
	"github.com/gitql/gitql/sql"
	"github.com/gitql/gitql/sql/expression"

	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	assert := assert.New(t)
	childSchema := sql.Schema{
		sql.Field{"col1", sql.String},
		sql.Field{"col2", sql.String},
		sql.Field{"col3", sql.Integer},
		sql.Field{"col4", sql.BigInteger},
	}
	child := mem.NewTable("test", childSchema)
	err := child.Insert(sql.NewRow("col1_1", "col2_1", int32(1111), int64(2222)))
	assert.Nil(err)
	err = child.Insert(sql.NewRow("col1_2", "col2_2", int32(3333), int64(4444)))
	assert.Nil(err)

	f := NewFilter(
		expression.NewEquals(
			expression.NewGetField(0, sql.String, "col1"),
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
		expression.NewGetField(2, sql.Integer, "col3"),
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
		expression.NewGetField(3, sql.BigInteger, "col4"),
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
