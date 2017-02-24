package expression

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/sql"

	"github.com/stretchr/testify/assert"
)

func TestStar(t *testing.T) {
	assert := assert.New(t)
	var e sql.Expression = NewStar()
	assert.NotNil(e)
	assert.Equal("*", e.Name())
}
