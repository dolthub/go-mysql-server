package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestUnresolvedTable(t *testing.T) {
	require := require.New(t)
	var n sql.Node = NewUnresolvedTable("test_table", "")
	require.NotNil(n)
}
