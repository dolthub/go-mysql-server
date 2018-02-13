package plan

import (
	"testing"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/stretchr/testify/assert"
)

func TestUnresolvedTable(t *testing.T) {
	assert := assert.New(t)
	var n sql.Node = NewUnresolvedTable("test_table")
	assert.NotNil(n)
}
