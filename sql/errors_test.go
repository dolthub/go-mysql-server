package sql

import (
	"fmt"
	"testing"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLErrorCast(t *testing.T) {
	integerRangeErr := ErrIntegerOutOfRange.New("BIGINT UNSIGNED", "(value + 1)")
	assert.Equal(t, "BIGINT UNSIGNED value is out of range in '(value + 1)'", integerRangeErr.Error())

	tests := []struct {
		err      error
		code     int
		sqlState string
	}{
		{ErrTableNotFound.New("table not found err"), mysql.ERNoSuchTable, ""},
		{integerRangeErr, mysql.ERDataOutOfRange, mysql.SSDataOutOfRange},
		{ErrInvalidType.New("unhandled mysql error"), mysql.ERUnknownError, ""},
		{fmt.Errorf("generic error"), mysql.ERUnknownError, ""},
		{nil, mysql.ERUnknownError, ""},
	}

	for _, test := range tests {
		var nilErr *mysql.SQLError = nil
		t.Run(fmt.Sprintf("%v %v", test.err, test.code), func(t *testing.T) {
			err := CastSQLError(test.err)
			if err != nil {
				require.Error(t, err)
				assert.Equal(t, err.Number(), test.code)
				if test.sqlState != "" {
					assert.Equal(t, test.sqlState, err.SQLState())
				}
			} else {
				assert.Equal(t, err, nilErr)
			}
		})
	}
}

// TestSQLErrorCastConstraintViolationSQLState verifies that integrity-
// constraint-violation errors (duplicate key, NOT NULL violation, foreign
// key violation) are cast to MySQL's SQLSTATE 23000, matching real MySQL's
// behavior. Before this fix these fell through to the driver default of
// SQLSTATE HY000 ("General error"), which prevents MySQL clients that
// branch on the SQLSTATE class to detect constraint violations (e.g. PHP's
// PDO, whose mysql driver maps 23000 to a dedicated
// IntegrityConstraintViolationException) from distinguishing a constraint
// violation from any other server error.
func TestSQLErrorCastConstraintViolationSQLState(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"primary key violation", ErrPrimaryKeyViolation.New("dup")},
		{"unique key violation", ErrUniqueKeyViolation.New("dup")},
		{"duplicate entry", ErrDuplicateEntry.New("dup")},
		{"not null violation", ErrInsertIntoNonNullableProvidedNull.New("col")},
		{"foreign key child violation", ErrForeignKeyChildViolation.New("fk", "child", "parent", "idx")},
		{"foreign key parent violation", ErrForeignKeyParentViolation.New("fk", "child", "parent", "idx")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := CastSQLError(test.err)
			require.NotNil(t, err)
			assert.Equal(t, mysql.SSConstraintViolation, err.SQLState(),
				"expected SQLSTATE 23000 (integrity constraint violation), matching MySQL")
		})
	}
}

// TestSQLErrorCastNoDefaultForField verifies that omitting a NOT NULL
// column with no default from an INSERT is cast to MySQL's real error code
// 1364 (ER_NO_DEFAULT_FOR_FIELD), not the generic 1105 (ER_UNKNOWN_ERROR)
// it fell through to before this fix. Unlike duplicate-key/FK violations,
// real MySQL 8.0 reports this under SQLSTATE HY000, not 23000 - some
// clients (e.g. Drupal's mysql driver) special-case the numeric code 1364
// directly for this reason, so getting the code right matters even though
// the SQLSTATE here intentionally stays HY000.
func TestSQLErrorCastNoDefaultForField(t *testing.T) {
	err := CastSQLError(ErrInsertIntoNonNullableDefaultNullColumn.New("age"))
	require.NotNil(t, err)
	assert.Equal(t, mysql.ERNoDefaultForField, err.Number())
	assert.Equal(t, "HY000", err.SQLState())
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
