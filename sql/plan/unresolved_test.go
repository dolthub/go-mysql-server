package plan

import (
	"testing"

	"github.com/gitql/gitql/sql"
	"github.com/stretchr/testify/assert"
)

func TestUnresolvedRelation(t *testing.T) {
	assert := assert.New(t)
	var r sql.Node = NewUnresolvedRelation("test_table")
	assert.NotNil(r)
}
