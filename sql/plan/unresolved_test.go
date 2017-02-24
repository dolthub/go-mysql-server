package plan

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/sql"

	"github.com/stretchr/testify/assert"
)

func TestUnresolvedTable(t *testing.T) {
	assert := assert.New(t)
	var n sql.Node = NewUnresolvedTable("test_table")
	assert.NotNil(n)
}
