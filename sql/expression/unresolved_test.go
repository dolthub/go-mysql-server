package expression

import (
	"testing"

	"github.com/mvader/gitql/sql"
	"github.com/stretchr/testify/assert"
)

func TestUnresolvedExpression(t *testing.T) {
	assert := assert.New(t)
	var e sql.Expression = NewUnresolvedColumn("test_col")
	assert.NotNil(e)
	var o sql.Expression = NewEquals(e, e)
	assert.NotNil(o)
	o = NewNot(e)
	assert.NotNil(o)
}
