package sql

import (
	"fmt"
	"testing"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLErrorCast(t *testing.T) {

	tests := []struct {
		err  error
		code int
	}{
		{ErrTableNotFound.New("table not found err"), mysql.ERNoSuchTable},
		{ErrInvalidType.New("unhandled mysql error"), mysql.ERUnknownError},
		{fmt.Errorf("generic error"), mysql.ERUnknownError},
		{nil, mysql.ERUnknownError},
	}

	for _, test := range tests {
		var nilErr *mysql.SQLError = nil
		t.Run(fmt.Sprintf("%v %v", test.err, test.code), func(t *testing.T) {
			err, _, ok := CastSQLError(test.err)
			if !ok {
				require.Error(t, err)
				assert.Equal(t, err.Number(), test.code)
			} else {
				assert.Equal(t, err, nilErr)
			}
		})
	}
}
