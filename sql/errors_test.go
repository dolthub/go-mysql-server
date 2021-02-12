package sql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLErrorCast(t *testing.T) {

	tests := []struct {
		err  error
		code int
	}{
		{ErrTableNotFound.New("table not found err"), 1146},
		{ErrInvalidType.New("unhandled mysql error"), 1105},
		{fmt.Errorf("generic error"), 1105},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.err, test.code), func(t *testing.T) {
			var err = CastSQLError(test.err)
			require.Error(t, err)
			assert.Equal(t, err.Number(), test.code)
		})
	}
}
