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
	"strings"
)

func GenerateCreateTableStatement(tblName string, colStmts []string, tblCharsetName, tblCollName string) string {
	return fmt.Sprintf(
		"CREATE TABLE %s (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
		QuoteIdentifier(tblName),
		strings.Join(colStmts, ",\n"),
		tblCharsetName,
		tblCollName,
	)
}

func GenerateCreateTableColumnDefinition(colName string, colType Type, nullable bool, autoInc bool, hasDefault bool, colDefault string, comment string) string {
	stmt := fmt.Sprintf("  %s %s", QuoteIdentifier(colName), colType.String())
	if !nullable {
		stmt = fmt.Sprintf("%s NOT NULL", stmt)
	}
	if autoInc {
		stmt = fmt.Sprintf("%s AUTO_INCREMENT", stmt)
	}
	if c, ok := colType.(SpatialColumnType); ok {
		if s, d := c.GetSpatialTypeSRID(); d {
			stmt = fmt.Sprintf("%s SRID %v", stmt, s)
		}
	}
	if hasDefault {
		stmt = fmt.Sprintf("%s DEFAULT %s", stmt, colDefault)
	}
	if comment != "" {
		stmt = fmt.Sprintf("%s COMMENT '%s'", stmt, comment)
	}
	return stmt
}

func GenerateCreateTablePrimaryKeyDefinition(pkCols []string) string {
	return fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(QuoteIdentifiers(pkCols), ","))
}

func GenerateCreateTableIndexDefinition(isUnique, isSpatial bool, indexID string, indexCols []string, comment string) string {
	unique := ""
	if isUnique {
		unique = "UNIQUE "
	}

	spatial := ""
	if isSpatial {
		unique = "SPATIAL "
	}
	key := fmt.Sprintf("  %s%sKEY %s (%s)", unique, spatial, QuoteIdentifier(indexID), strings.Join(indexCols, ","))
	if comment != "" {
		key = fmt.Sprintf("%s COMMENT '%s'", key, comment)
	}
	return key
}

func GenerateCreateTableForiegnKeyDefinition(fkName string, fkCols []string, parentTbl string, parentCols []string, onDelete, onUpdate string) string {
	keyCols := strings.Join(QuoteIdentifiers(fkCols), ",")
	refCols := strings.Join(QuoteIdentifiers(parentCols), ",")
	fkey := fmt.Sprintf("  CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)", QuoteIdentifier(fkName), keyCols, QuoteIdentifier(parentTbl), refCols)
	if onDelete != "" {
		fkey = fmt.Sprintf("%s ON DELETE %s", fkey, onDelete)
	}
	if onUpdate != "" {
		fkey = fmt.Sprintf("%s ON UPDATE %s", fkey, onUpdate)
	}
	return fkey
}

func GenerateCreateTableCheckConstraintClause(checkName, checkExpr string, enforced bool) string {
	cc := fmt.Sprintf("  CONSTRAINT %s CHECK (%s)", QuoteIdentifier(checkName), checkExpr)
	if !enforced {
		cc = fmt.Sprintf("%s /*!80016 NOT ENFORCED */", cc)
	}
	return cc
}

// QuoteIdentifier wraps the specified identifier in backticks and escapes all occurrences of backticks in the
// identifier by replacing them with double backticks.
func QuoteIdentifier(id string) string {
	id = strings.ReplaceAll(id, "`", "``")
	return fmt.Sprintf("`%s`", id)
}

// QuoteIdentifiers wraps each of the specified identifiers in backticks, escapes all occurrences of backticks in
// the identifier, and returns a slice of the quoted identifiers.
func QuoteIdentifiers(ids []string) []string {
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = QuoteIdentifier(id)
	}
	return quoted
}
