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

package planbuilder

import (
	"github.com/dolthub/vitess/go/mysql"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	errInvalidDescribeFormat = errors.NewKind("invalid format %q for DESCRIBE, supported formats: %s")

	errInvalidSortOrder = errors.NewKind("invalid sort order: %s")

	ErrPrimaryKeyOnNullField = errors.NewKind("All parts of PRIMARY KEY must be NOT NULL")

	// ErrSelectsDifferentLength is returned when the two sides of a
	// UNION do not have the same number of columns in their schemas.
	ErrSelectsDifferentLength = errors.NewKind(
		"the used SELECT statements have a different number of columns; left has %d column(s) right has %d column(s).",
	)

	ErrQualifiedOrderBy = errors.NewKind("Table '%s' from one of the SELECTs cannot be used in global ORDER clause")

	ErrOrderByBinding = errors.NewKind("bindings in sort clauses not supported yet")

	ErrFailedToParseStats = errors.NewKind("failed to parse data: %s\n%s")

	// errMySQLDistinctWindow is returned for DISTINCT aggregate windows unsupported by MySQL.
	errMySQLDistinctWindow = mysql.NewSQLError(mysql.ERNotSupportedYet, mysql.SSClientError,
		"This version of MySQL doesn't yet support '<window function>(DISTINCT ..)'")

	// errMySQLDistinctStarWindow is returned for COUNT(DISTINCT *) windows, which are invalid MySQL syntax.
	errMySQLDistinctStarWindow = mysql.NewSQLError(mysql.ERParseError, mysql.SSClientError,
		"You have an error in your SQL syntax")
)
