package function

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TestLoadFileNonStringArg is a regression test for a panic where a non-string
// argument to LOAD_FILE (e.g. LOAD_FILE(1)) triggered
// "interface conversion: interface {} is int8, not string".
// MySQL coerces the argument to a string and returns NULL when no such file
// exists, so the function must not panic.
func TestLoadFileNonStringArg(t *testing.T) {
	// Make sure secure_file_priv does not point at a real directory from a
	// previous test, so getFile takes the plain os.Open path.
	vars := map[string]interface{}{"secure_file_priv": ""}
	require.NoError(t, sql.SystemVariables.AssignValues(vars))

	// Integer argument: coerced to the filename "1", which does not exist.
	fn := NewLoadFile(sql.NewEmptyContext(), expression.NewLiteral(int8(1), types.Int8))
	res, err := fn.Eval(sql.NewEmptyContext(), sql.Row{})
	assert.NoError(t, err)
	assert.Nil(t, res)

	// NULL argument yields NULL.
	fnNull := NewLoadFile(sql.NewEmptyContext(), expression.NewLiteral(nil, types.Null))
	resNull, err := fnNull.Eval(sql.NewEmptyContext(), sql.Row{})
	assert.NoError(t, err)
	assert.Nil(t, resNull)
}
