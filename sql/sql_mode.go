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
	"sort"
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"
)

const (
	SqlModeSessionVar = "SQL_MODE"

	ANSI                 = "ANSI"
	ANSIQuotes           = "ANSI_QUOTES"
	OnlyFullGroupBy      = "ONLY_FULL_GROUP_BY"
	NoAutoValueOnZero    = "NO_AUTO_VALUE_ON_ZERO"
	NoEngineSubstitution = "NO_ENGINE_SUBSTITUTION"
	StrictTransTables    = "STRICT_TRANS_TABLES"
	DefaultSqlMode       = "NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES"
)

var defaultMode *SqlMode

func init() {
	elements := strings.Split(strings.ToLower(DefaultSqlMode), ",")
	sort.Strings(elements)
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
// has a comma delimited list of SQL modes (e.g. "ONLY_FULLY_GROUP_BY,ANSI_QUOTES").
func NewSqlModeFromString(sqlModeString string) *SqlMode {
	if sqlModeString == DefaultSqlMode {
		return defaultMode
	}
	sqlModeString = strings.ToLower(sqlModeString)
	elements := strings.Split(sqlModeString, ",")
	sort.Strings(elements)
	modes := map[string]struct{}{}
	for _, element := range elements {
		modes[element] = struct{}{}
	}

	return &SqlMode{
		modes:      modes,
		modeString: strings.ToUpper(strings.Join(elements, ",")),
	}
}

// AnsiQuotes returns true if the ANSI_QUOTES SQL mode is enabled. Note that the ANSI mode is a compound mode that
// includes ANSI_QUOTES and other options, so if ANSI or ANSI_QUOTES is enabled, this function will return true.
func (s *SqlMode) AnsiQuotes() bool {
	return s.ModeEnabled(ANSIQuotes) || s.ModeEnabled(ANSI)
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
		AnsiQuotes: s.AnsiQuotes(),
	}
}

// String returns the SQL_MODE string representing this SqlMode instance.
func (s *SqlMode) String() string {
	return s.modeString
}
