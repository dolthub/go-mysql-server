package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestUnresolvedTable(t *testing.T) {
	require := require.New(t)
	var n sql.Node = NewUnresolvedTable("test_table", "")
	require.NotNil(n)
}
