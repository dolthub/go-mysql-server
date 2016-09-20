package expression

import (
	"testing"

	"github.com/mvader/gitql/mem"
	"github.com/mvader/gitql/sql"
	"github.com/stretchr/testify/assert"
)

func TestExpressions(t *testing.T) {
	assert := assert.New(t)
	childSchema := sql.Schema{
		sql.Field{"col1", sql.String},
		sql.Field{"col2", sql.String},
		sql.Field{"col3", sql.Integer},
		sql.Field{"col4", sql.Boolean},
		sql.Field{"col5", sql.Integer},
	}
	child := mem.NewTable("test", childSchema)
	child.Insert("col1_1", "col2_1", 111, false, 111)
	child.Insert("col1_2", "col2_2", 222, true, 111)
	child.Insert("col1_3", "col2_3", 333, true, 111)
	child.Insert("col1_4", "col2_4", 111, false, 111)

	eq := NewEquals(NewGetField(2, sql.Integer), NewLiteral(111, sql.Integer))
	dis := NewEquals(NewNot(NewGetField(3, sql.Boolean)),
		NewEquals(NewGetField(2, sql.Integer), NewGetField(4, sql.Integer)))
	iter, err := child.RowIter()

	assert.Nil(err)

	row, err := iter.Next()

	assert.Equal(eq.Eval(row), true)
	assert.Equal(dis.Eval(row), true)
	row, err = iter.Next()

	assert.Equal(eq.Eval(row), false)
	assert.Equal(dis.Eval(row), true)
}
