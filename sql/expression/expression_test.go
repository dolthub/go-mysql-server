package expression

import (
	"testing"

	"github.com/mvader/gitql/sql"
	"github.com/stretchr/testify/assert"
)

func TestExpressions(t *testing.T) {
	assert := assert.New(t)
	row1 := sql.NewMemoryRow("col1_1", "col2_1", int32(111), false, int32(111))
	row2 := sql.NewMemoryRow("col1_2", "col2_2", int32(222), true, int32(111))

	eq := NewEquals(NewGetField(2, sql.Integer, "col3"), NewLiteral(int32(111), sql.Integer))

	not, err := NewNot(NewGetField(3, sql.Boolean, "col4"))

	assert.Nil(err)

	dis := NewEquals(not, NewEquals(NewGetField(2, sql.Integer, "col3"), NewGetField(4, sql.Integer, "col5")))

	assert.Equal(eq.Eval(row1), true)
	assert.Equal(dis.Eval(row1), true)

	assert.Equal(eq.Eval(row2), false)
	assert.Equal(dis.Eval(row2), true)
}
