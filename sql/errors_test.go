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
			err := CastSQLError(test.err)
			if err != nil {
				require.Error(t, err)
				assert.Equal(t, err.Number(), test.code)
			} else {
				assert.Equal(t, err, nilErr)
			}
		})
	}
}

func TestWrappedInsertError(t *testing.T) {
	tests := []struct {
		err             error
		expectedErrStrs []string
	}{
		{
			err: ErrInvalidType.New("unhandled mysql error"),
			expectedErrStrs: []string{
				"TestWrappedInsertError",              // contains stack trace from this method
				"invalid type: unhandled mysql error", // contains the wrapped error
			},
		},
		{
			err: fmt.Errorf("generic error"),
			expectedErrStrs: []string{
				"generic error", // contains the wrapped error
			},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.err), func(t *testing.T) {
			r := Row{"a", "b"}
			err := NewWrappedInsertError(r, test.err)
			require.Error(t, err)
			extendedOutput := fmt.Sprintf("%+v", err)
			for _, expectedErrStr := range test.expectedErrStrs {
				assert.Contains(t, extendedOutput, expectedErrStr)
			}
		})
	}
}
