// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"
)

const (
	SqlModeSessionVar = "sql_mode"

	REAL_AS_FLOAT              = "REAL_AS_FLOAT"
	PIPES_AS_CONCAT            = "PIPES_AS_CONCAT"
	ANSI_QUOTES                = "ANSI_QUOTES"
	IGNORE_SPACE               = "IGNORE_SPACE"
	NOT_USED                   = "NOT_USED"
	ONLY_FULL_GROUP_BY         = "ONLY_FULL_GROUP_BY"
	NO_UNSIGNED_SUBTRACTION    = "NO_UNSIGNED_SUBTRACTION"
	NO_DIR_IN_CREATE           = "NO_DIR_IN_CREATE"
	NO_AUTO_CREATE_USER        = "NO_AUTO_CREATE_USER" // TODO: this is deprecated, have it take up NOT_USED_9 for now
	NOT_USED_10                = "NOT_USED_10"
	NOT_USED_11                = "NOT_USED_11"
	NOT_USED_12                = "NOT_USED_12"
	NOT_USED_13                = "NOT_USED_13"
	NOT_USED_14                = "NOT_USED_14"
	NOT_USED_15                = "NOT_USED_15"
	NOT_USED_16                = "NOT_USED_16"
	NOT_USED_17                = "NOT_USED_17"
	NOT_USED_18                = "NOT_USED_18"
	ANSI                       = "ANSI" // Includes REAL_AS_FLOAT, PIPES_AS_CONCAT, ANSI_QUOTES, IGNORE_SPACE, and ONLY_FULL_GROUP_BY
	NO_AUTO_VALUE_ON_ZERO      = "NO_AUTO_VALUE_ON_ZERO"
	NO_BACKSLASH_ESCAPES       = "NO_BACKSLASH_ESCAPES"
	STRICT_TRANS_TABLES        = "STRICT_TRANS_TABLES"
	STRICT_ALL_TABLES          = "STRICT_ALL_TABLES"
	NO_ZERO_IN_DATE            = "NO_ZERO_IN_DATE"
	NO_ZERO_DATE               = "NO_ZERO_DATE"
	ALLOW_INVALID_DATES        = "ALLOW_INVALID_DATES"
	ERROR_FOR_DIVISION_BY_ZERO = "ERROR_FOR_DIVISION_BY_ZERO"
	TRADITIONAL                = "TRADITIONAL" // includes STRICT_TRANS_TABLES, STRICT_ALL_TABLES, NO_ZERO_IN_DATE, ERROR_FOR_DIVISION_BY_ZERO, and NO_ENGINE_SUBSTITUTION
	NOT_USED_29                = "NOT_USED_29"
	HIGH_NOT_PRECEDENCE        = "HIGH_NOT_PRECEDENCE"
	NO_ENGINE_SUBSTITUTION     = "NO_ENGINE_SUBSTITUTION"
	PAD_CHAR_TO_FULL_LENGTH    = "PAD_CHAR_TO_FULL_LENGTH"
	TIME_TRUNCATE_FRACTIONAL   = "TIME_TRUNCATE_FRACTIONAL"
	INTERPRET_UTF8_AS_UTF8MB4  = "INTERPRET_UTF8_AS_UTF8MB4"
)

// Bits for different SQL modes
// https://github.com/mysql/mysql-server/blob/f362272c18e856930c867952a7dd1d840cbdb3b1/sql/system_variables.h#L123-L173
const (
	MODE_REAL_AS_FLOAT              = 0x1
	MODE_PIPES_AS_CONCAT            = 0x1 << 1
	MODE_ANSI_QUOTES                = 0x1 << 2
	MODE_IGNORE_SPACE               = 0x1 << 3
	MODE_NOT_USED                   = 0x1 << 4
	MODE_ONLY_FULL_GROUP_BY         = 0x1 << 5
	MODE_NO_UNSIGNED_SUBTRACTION    = 0x1 << 6
	MODE_NO_DIR_IN_CREATE           = 0x1 << 7
	MODE_NO_AUTO_CREATE_USER        = 0x1 << 8
	MODE_NOT_USED_10                = 0x1 << 9
	MODE_NOT_USED_11                = 0x1 << 10
	MODE_NOT_USED_12                = 0x1 << 11
	MODE_NOT_USED_13                = 0x1 << 12
	MODE_NOT_USED_14                = 0x1 << 13
	MODE_NOT_USED_15                = 0x1 << 14
	MODE_NOT_USED_16                = 0x1 << 15
	MODE_NOT_USED_17                = 0x1 << 16
	MODE_NOT_USED_18                = 0x1 << 17
	MODE_ANSI                       = 0x1 << 18
	MODE_NO_AUTO_VALUE_ON_ZERO      = 0x1 << 19
	MODE_NO_BACKSLASH_ESCAPES       = 0x1 << 20
	MODE_STRICT_TRANS_TABLES        = 0x1 << 21
	MODE_STRICT_ALL_TABLES          = 0x1 << 22
	MODE_NO_ZERO_IN_DATE            = 0x1 << 23
	MODE_NO_ZERO_DATE               = 0x1 << 24
	MODE_ALLOW_INVALID_DATES        = 0x1 << 25
	MODE_ERROR_FOR_DIVISION_BY_ZERO = 0x1 << 26
	MODE_TRADITIONAL                = 0x1 << 27
	MODE_NOT_USED_29                = 0x1 << 28
	MODE_HIGH_NOT_PRECEDENCE        = 0x1 << 29
	MODE_NO_ENGINE_SUBSTITUTION     = 0x1 << 30
	MODE_PAD_CHAR_TO_FULL_LENGTH    = 0x1 << 31
	MODE_TIME_TRUNCATE_FRACTIONAL   = 0x1 << 32
	MODE_INTERPRET_UTF8_AS_UTF8MB4  = 0x1 << 33

	// MODE_IGNORED_MASK contains deprecated/obsolete SQL mode bits that can be safely ignored
	// during binlog replication. These modes existed in older MySQL versions but are no longer used.
	// See: https://github.com/mysql/mysql-server/blob/trunk/sql/system_variables.h MODE_IGNORED_MASK
	MODE_IGNORED_MASK = 0x00100 | // was: MODE_POSTGRESQL
		0x00200 | // was: MODE_ORACLE
		0x00400 | // was: MODE_MSSQL
		0x00800 | // was: MODE_DB2
		0x01000 | // was: MODE_MAXDB
		0x02000 | // was: MODE_NO_KEY_OPTIONS
		0x04000 | // was: MODE_NO_TABLE_OPTIONS
		0x08000 | // was: MODE_NO_FIELD_OPTIONS
		0x10000 | // was: MODE_MYSQL323
		0x20000 | // was: MODE_MYSQL40
		0x10000000 // was: MODE_NO_AUTO_CREATE_USER
)

// sqlModeBitMap maps SQL mode bit flags to their string names.
// Note: MODE_NO_AUTO_CREATE_USER is NOT in this map - it's in MODE_IGNORED_MASK and filtered out
var sqlModeBitMap = map[uint64]string{
	MODE_REAL_AS_FLOAT:              REAL_AS_FLOAT,
	MODE_PIPES_AS_CONCAT:            PIPES_AS_CONCAT,
	MODE_ANSI_QUOTES:                ANSI_QUOTES,
	MODE_IGNORE_SPACE:               IGNORE_SPACE,
	MODE_NOT_USED:                   NOT_USED,
	MODE_ONLY_FULL_GROUP_BY:         ONLY_FULL_GROUP_BY,
	MODE_NO_UNSIGNED_SUBTRACTION:    NO_UNSIGNED_SUBTRACTION,
	MODE_NO_DIR_IN_CREATE:           NO_DIR_IN_CREATE,
	MODE_NO_AUTO_CREATE_USER:        NO_AUTO_CREATE_USER,
	MODE_NOT_USED_10:                NOT_USED_10,
	MODE_NOT_USED_11:                NOT_USED_11,
	MODE_NOT_USED_12:                NOT_USED_12,
	MODE_NOT_USED_13:                NOT_USED_13,
	MODE_NOT_USED_14:                NOT_USED_14,
	MODE_NOT_USED_15:                NOT_USED_15,
	MODE_NOT_USED_16:                NOT_USED_16,
	MODE_NOT_USED_17:                NOT_USED_17,
	MODE_NOT_USED_18:                NOT_USED_18,
	MODE_ANSI:                       ANSI,
	MODE_NO_AUTO_VALUE_ON_ZERO:      NO_AUTO_VALUE_ON_ZERO,
	MODE_NO_BACKSLASH_ESCAPES:       NO_BACKSLASH_ESCAPES,
	MODE_STRICT_TRANS_TABLES:        STRICT_TRANS_TABLES,
	MODE_STRICT_ALL_TABLES:          STRICT_ALL_TABLES,
	MODE_NO_ZERO_IN_DATE:            NO_ZERO_IN_DATE,
	MODE_NO_ZERO_DATE:               NO_ZERO_DATE,
	MODE_ALLOW_INVALID_DATES:        ALLOW_INVALID_DATES,
	MODE_ERROR_FOR_DIVISION_BY_ZERO: ERROR_FOR_DIVISION_BY_ZERO,
	MODE_TRADITIONAL:                TRADITIONAL,
	MODE_NOT_USED_29:                NOT_USED_29,
	MODE_HIGH_NOT_PRECEDENCE:        HIGH_NOT_PRECEDENCE,
	MODE_NO_ENGINE_SUBSTITUTION:     NO_ENGINE_SUBSTITUTION,
	MODE_PAD_CHAR_TO_FULL_LENGTH:    PAD_CHAR_TO_FULL_LENGTH,
	MODE_TIME_TRUNCATE_FRACTIONAL:   TIME_TRUNCATE_FRACTIONAL,
	MODE_INTERPRET_UTF8_AS_UTF8MB4:  INTERPRET_UTF8_AS_UTF8MB4,
}

var DefaultSqlMode = strings.Join([]string{
	ONLY_FULL_GROUP_BY,
	STRICT_TRANS_TABLES,
	NO_ZERO_IN_DATE,
	NO_ZERO_DATE,
	ERROR_FOR_DIVISION_BY_ZERO,
	NO_ENGINE_SUBSTITUTION,
}, ",")

var defaultMode *SqlMode

func init() {
	elements := strings.Split(strings.ToLower(DefaultSqlMode), ",")
	modes := map[string]struct{}{}
	for _, element := range elements {
		modes[element] = struct{}{}
	}
	defaultMode = &SqlMode{
		modes:      modes,
		modeString: DefaultSqlMode,
	}
}

// SqlMode encodes the SQL mode string and provides methods for querying the enabled modes.
type SqlMode struct {
	modes      map[string]struct{}
	modeString string
}

// LoadSqlMode loads the SQL mode using the session data contained in |ctx| and returns a SqlMode
// instance that can be used to query which modes are enabled.
func LoadSqlMode(ctx *Context) *SqlMode {
	sqlMode, err := ctx.Session.GetSessionVariable(ctx, SqlModeSessionVar)
	if err != nil {
		// if system variables are not initialized, assume default sqlMode
		return &SqlMode{modes: nil, modeString: ""}
	}

	sqlModeString, ok := sqlMode.(string)
	if !ok {
		ctx.GetLogger().Warnf("sqlMode system variable value is invalid: '%v'", sqlMode)
		return &SqlMode{modes: nil, modeString: ""}
	}

	return NewSqlModeFromString(sqlModeString)
}

// NewSqlModeFromString returns a new SqlMode instance, constructed from the specified |sqlModeString| that
// has a comma-delimited list of SQL modes (e.g. "ONLY_FULLY_GROUP_BY,ANSI_QUOTES").
func NewSqlModeFromString(sqlModeString string) *SqlMode {
	if sqlModeString == DefaultSqlMode {
		return defaultMode
	}
	sqlModeString = strings.ToLower(sqlModeString)

	modes := map[string]struct{}{}
	start, end := 0, 0
	for ; end < len(sqlModeString); end++ {
		if sqlModeString[end] != ',' {
			continue
		}
		modes[sqlModeString[start:end]] = struct{}{}
		start = end + 1
	}
	modes[sqlModeString[start:end]] = struct{}{}

	return &SqlMode{
		modes: modes,
	}
}

// AnsiQuotes returns true if the ANSI_QUOTES SQL mode is enabled. Note that the ANSI mode is a compound mode that
// includes ANSI_QUOTES and other options, so if ANSI or ANSI_QUOTES is enabled, this function will return true.
func (s *SqlMode) AnsiQuotes() bool {
	return s.ModeEnabled(ANSI_QUOTES) || s.ModeEnabled(ANSI)
}

// OnlyFullGroupBy returns true is ONLY_TRUE_GROUP_BY SQL mode is enabled. Note that ANSI mode is a compound mode that
// includes ONLY_FULL_GROUP_BY and other options, so if ANSI or ONLY_TRUE_GROUP_BY is enabled, this function will
// return true.
func (s *SqlMode) OnlyFullGroupBy() bool {
	return s.ModeEnabled(ONLY_FULL_GROUP_BY) || s.ModeEnabled(ANSI)
}

// PipesAsConcat returns true if PIPES_AS_CONCAT SQL mode is enabled. Note that ANSI mode is a compound mode that
// includes PIPES_AS_CONCAT and other options, so if ANSI or PIPES_AS_CONCAT is enabled, this function will return true.
func (s *SqlMode) PipesAsConcat() bool {
	return s.ModeEnabled(PIPES_AS_CONCAT) || s.ModeEnabled(ANSI)
}

// StrictTransTables returns true if STRICT_TRANS_TABLES SQL mode is enabled. Note that TRADITIONAL mode is a compound
// mode that includes STRICT_TRANS_TABLES and other options, so if TRADITIONAL or STRICT_TRANS_TABLES is enabled, this
// function will return true.
func (s *SqlMode) StrictTransTables() bool {
	return s.ModeEnabled(STRICT_TRANS_TABLES) || s.ModeEnabled(TRADITIONAL)
}

// StrictAllTables returns true if STRICT_ALL_TABLES SQL mode is enabled. Note that TRADITIONAL mode is a compound
// mode that includes STRICT_ALL_TABLES and other options, so if TRADITIONAL or STRICT_ALL_TABLES is enabled, this
// function will return true.
func (s *SqlMode) StrictAllTables() bool {
	return s.ModeEnabled(STRICT_ALL_TABLES) || s.ModeEnabled(TRADITIONAL)
}

// Strict mode is enabled when either STRICT_TRANS_TABLES or STRICT_ALL_TABLES is enabled.
func (s *SqlMode) Strict() bool {
	return s.StrictAllTables() || s.StrictTransTables()
}

// ModeEnabled returns true if |mode| was explicitly specified in the SQL_MODE string that was used to
// create this SqlMode instance. Note this function does not support expanding compound modes into the
// individual modes they contain (e.g. if "ANSI" is the SQL_MODE string, then this function will not
// report that "ANSI_QUOTES" is enabled). To deal with compound modes, use the mode specific functions,
// such as SqlMode::AnsiQuotes().
func (s *SqlMode) ModeEnabled(mode string) bool {
	_, ok := s.modes[strings.ToLower(mode)]
	return ok
}

// ParserOptions returns a ParserOptions struct, with options set based on what SQL modes are enabled.
func (s *SqlMode) ParserOptions() sqlparser.ParserOptions {
	return sqlparser.ParserOptions{
		AnsiQuotes:    s.AnsiQuotes(),
		PipesAsConcat: s.PipesAsConcat(),
	}
}

// String returns the SQL_MODE string representing this SqlMode instance.
func (s *SqlMode) String() string {
	if len(s.modeString) == 0 && len(s.modes) > 0 {
		// TODO: this should be sorted as it appears in ./sql/variables/system_variables.go
		modes := make([]string, 0, len(s.modes))
		for mode := range s.modes {
			modes = append(modes, mode)
		}
		s.modeString = strings.ToUpper(strings.Join(modes, ","))
	}
	return s.modeString
}

// ConvertSqlModeBitmask converts sql_mode values to their string representation.
func ConvertSqlModeBitmask(val any) (string, error) {
	var bitmask uint64
	switch v := val.(type) {
	case []byte:
		if n, err := strconv.ParseUint(string(v), 10, 64); err == nil {
			bitmask = n
		}
	case int8:
		bitmask = uint64(v)
	case int16:
		bitmask = uint64(v)
	case int:
		bitmask = uint64(v)
	case int32:
		bitmask = uint64(v)
	case int64:
		bitmask = uint64(v)
	case uint8:
		bitmask = uint64(v)
	case uint16:
		bitmask = uint64(v)
	case uint:
		bitmask = uint64(v)
	case uint32:
		bitmask = uint64(v)
	case uint64:
		bitmask = v
	default:
		return fmt.Sprintf("%v", val), nil
	}

	bitmask = bitmask &^ MODE_IGNORED_MASK

	var modes []string
	var matchedBits uint64
	for bit, modeName := range sqlModeBitMap {
		if bitmask&bit != 0 {
			modes = append(modes, modeName)
			matchedBits |= bit
		}
	}

	if bitmask != 0 && matchedBits != bitmask {
		return fmt.Sprintf("%v", val), nil
	}

	if len(modes) == 0 {
		return "", nil
	}

	return strings.Join(modes, ","), nil
}
