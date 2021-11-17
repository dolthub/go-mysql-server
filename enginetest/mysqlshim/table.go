// Copyright 2021 Dolthub, Inc.
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

package mysqlshim

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// Table represents a table for a local MySQL server.
type Table struct {
	db   Database
	name string
}

var _ sql.Table = Table{}
var _ sql.InsertableTable = Table{}
var _ sql.UpdatableTable = Table{}
var _ sql.DeletableTable = Table{}
var _ sql.ReplaceableTable = Table{}
var _ sql.TruncateableTable = Table{}
var _ sql.IndexAddressableTable = Table{}
var _ sql.AlterableTable = Table{}
var _ sql.IndexAlterableTable = Table{}
var _ sql.IndexedTable = Table{}
var _ sql.ForeignKeyAlterableTable = Table{}
var _ sql.ForeignKeyTable = Table{}
var _ sql.CheckAlterableTable = Table{}
var _ sql.CheckTable = Table{}
var _ sql.StatisticsTable = Table{}
var _ sql.PrimaryKeyAlterableTable = Table{}

// Name implements the interface sql.Table.
func (t Table) Name() string {
	return t.name
}

// String implements the interface sql.Table.
func (t Table) String() string {
	return t.name
}

// Schema implements the interface sql.Table.
func (t Table) Schema() sql.Schema {
	createTable, err := t.getCreateTable()
	if err != nil {
		panic(err)
	}
	return createTable.Schema()
}

// Partitions implements the interface sql.Table.
func (t Table) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return &tablePartitionIter{}, nil
}

// PartitionRows implements the interface sql.Table.
func (t Table) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return t.db.shim.Query(t.db.name, fmt.Sprintf("SELECT * FROM `%s`;", t.name))
}

// Inserter implements the interface sql.InsertableTable.
func (t Table) Inserter(ctx *sql.Context) sql.RowInserter {
	return &tableEditor{t, t.Schema()}
}

// Updater implements the interface sql.UpdatableTable.
func (t Table) Updater(ctx *sql.Context) sql.RowUpdater {
	return &tableEditor{t, t.Schema()}
}

// Deleter implements the interface sql.DeletableTable.
func (t Table) Deleter(ctx *sql.Context) sql.RowDeleter {
	return &tableEditor{t, t.Schema()}
}

// Replacer implements the interface sql.ReplaceableTable.
func (t Table) Replacer(ctx *sql.Context) sql.RowReplacer {
	return &tableEditor{t, t.Schema()}
}

// Truncate implements the interface sql.TruncateableTable.
func (t Table) Truncate(ctx *sql.Context) (int, error) {
	rows, err := t.db.shim.QueryRows(t.db.name, fmt.Sprintf("SELECT COUNT(*) FROM `%s`;", t.name))
	if err != nil {
		return 0, err
	}
	rowCount, err := sql.Int64.Convert(rows[0][0])
	if err != nil {
		return 0, err
	}
	err = t.db.shim.Exec("", fmt.Sprintf("TRUNCATE TABLE `%s`;", t.name))
	return int(rowCount.(int64)), err
}

// WithIndexLookup implements the interface sql.IndexAddressableTable.
func (t Table) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	return t
}

// AddColumn implements the interface sql.AlterableTable.
func (t Table) AddColumn(ctx *sql.Context, column *sql.Column, order *sql.ColumnOrder) error {
	statement := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s;", t.name, column.Name, strings.ToUpper(column.Type.String()))
	if !column.Nullable {
		statement = fmt.Sprintf("%s NOT NULL", statement)
	}
	if column.AutoIncrement {
		statement = fmt.Sprintf("%s AUTO_INCREMENT", statement)
	}
	if column.Default != nil {
		statement = fmt.Sprintf("%s DEFAULT %s", statement, column.Default.String())
	}
	if column.Comment != "" {
		statement = fmt.Sprintf("%s COMMENT '%s'", statement, column.Comment)
	}
	if order != nil {
		if order.First {
			statement = fmt.Sprintf("%s FIRST", statement)
		} else if len(order.AfterColumn) > 0 {
			statement = fmt.Sprintf("%s AFTER `%s`", statement, order.AfterColumn)
		}
	}
	return t.db.shim.Exec(t.db.name, statement)
}

// DropColumn implements the interface sql.AlterableTable.
func (t Table) DropColumn(ctx *sql.Context, columnName string) error {
	return t.db.shim.Exec(t.db.name, fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `%s`;", t.name, columnName))
}

// ModifyColumn implements the interface sql.AlterableTable.
func (t Table) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
	statement := fmt.Sprintf("ALTER TABLE `%s` CHANGE COLUMN `%s` `%s` %s;", t.name, columnName, column.Name, strings.ToUpper(column.Type.String()))
	if !column.Nullable {
		statement = fmt.Sprintf("%s NOT NULL", statement)
	}
	if column.AutoIncrement {
		statement = fmt.Sprintf("%s AUTO_INCREMENT", statement)
	}
	if column.Default != nil {
		statement = fmt.Sprintf("%s DEFAULT %s", statement, column.Default.String())
	}
	if column.Comment != "" {
		statement = fmt.Sprintf("%s COMMENT '%s'", statement, column.Comment)
	}
	if order != nil {
		if order.First {
			statement = fmt.Sprintf("%s FIRST", statement)
		} else if len(order.AfterColumn) > 0 {
			statement = fmt.Sprintf("%s AFTER `%s`", statement, order.AfterColumn)
		}
	}
	return t.db.shim.Exec(t.db.name, statement)
}

// CreateIndex implements the interface sql.IndexAlterableTable.
func (t Table) CreateIndex(ctx *sql.Context, indexName string, using sql.IndexUsing, constraint sql.IndexConstraint, columns []sql.IndexColumn, comment string) error {
	statement := "CREATE"
	switch constraint {
	case sql.IndexConstraint_Unique:
		statement += " UNIQUE INDEX"
	case sql.IndexConstraint_Fulltext:
		statement += " FULLTEXT INDEX"
	case sql.IndexConstraint_Spatial:
		statement += " SPATIAL INDEX"
	default:
		statement += " INDEX"
	}
	idxColumnNames := make([]string, len(columns))
	for i, column := range columns {
		idxColumnNames[i] = column.Name
	}
	if len(indexName) == 0 {
		indexName = randString(10)
	}
	statement = fmt.Sprintf("%s `%s` ON `%s` (`%s`)", statement, indexName, t.name, strings.Join(idxColumnNames, "`,`"))
	if len(comment) > 0 {
		statement = fmt.Sprintf("%s COMMENT '%s'", statement, strings.ReplaceAll(comment, "'", `\'`))
	}
	return t.db.shim.Exec(t.db.name, statement)
}

// DropIndex implements the interface sql.IndexAlterableTable.
func (t Table) DropIndex(ctx *sql.Context, indexName string) error {
	return t.db.shim.Exec(t.db.name, fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`;", t.name, indexName))
}

// RenameIndex implements the interface sql.IndexAlterableTable.
func (t Table) RenameIndex(ctx *sql.Context, fromIndexName string, toIndexName string) error {
	return t.db.shim.Exec(t.db.name, fmt.Sprintf("ALTER TABLE `%s` RENAME INDEX `%s` TO `%s`;", t.name, fromIndexName, toIndexName))
}

// GetIndexes implements the interface sql.IndexedTable.
func (t Table) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	//TODO: add this along with some kind of index implementation
	return nil, nil
}

// CreateForeignKey implements the interface sql.ForeignKeyAlterableTable.
func (t Table) CreateForeignKey(ctx *sql.Context, fkName string, columns []string, referencedTable string, referencedColumns []string, onUpdate, onDelete sql.ForeignKeyReferenceOption) error {
	constraint := ""
	if len(fkName) > 0 {
		constraint = fmt.Sprintf(" CONSTRAINT `%s`", fkName)
	}
	onDeleteStr := ""
	if onDelete != sql.ForeignKeyReferenceOption_DefaultAction {
		onDeleteStr = fmt.Sprintf(" ON DELETE %s", string(onDelete))
	}
	onUpdateStr := ""
	if onUpdate != sql.ForeignKeyReferenceOption_DefaultAction {
		onUpdateStr = fmt.Sprintf(" ON UPDATE %s", string(onUpdate))
	}
	return t.db.shim.Exec(t.db.name, fmt.Sprintf("ALTER TABLE `%s` ADD%s FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`)%s%s;",
		t.name, constraint, strings.Join(columns, "`,`"), referencedTable, strings.Join(referencedColumns, "`,`"), onDeleteStr, onUpdateStr))
}

// DropForeignKey implements the interface sql.ForeignKeyAlterableTable.
func (t Table) DropForeignKey(ctx *sql.Context, fkName string) error {
	return t.db.shim.Exec(t.db.name, fmt.Sprintf("ALTER TABLE `%s` DROP FOREIGN KEY `%s`;", t.name, fkName))
}

// GetForeignKeys implements the interface sql.ForeignKeyTable.
func (t Table) GetForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	//TODO: add this
	return nil, nil
}

// CreateCheck implements the interface sql.CheckAlterableTable.
func (t Table) CreateCheck(ctx *sql.Context, check *sql.CheckDefinition) error {
	statement := fmt.Sprintf("ALTER TABLE `%s` ADD", t.name)
	if len(check.Name) > 0 {
		statement = fmt.Sprintf("%s CONSTRAINT `%s`", statement, check.Name)
	}
	statement = fmt.Sprintf("%s CHECK (%s)", statement, check.CheckExpression)
	if !check.Enforced {
		statement = fmt.Sprintf("%s NOT ENFORCED", statement)
	}
	return t.db.shim.Exec(t.db.name, statement)
}

// DropCheck implements the interface sql.CheckAlterableTable.
func (t Table) DropCheck(ctx *sql.Context, chName string) error {
	return t.db.shim.Exec(t.db.name, fmt.Sprintf("ALTER TABLE `%s` DROP CHECK `%s`;", t.name, chName))
}

// GetChecks implements the interface sql.CheckTable.
func (t Table) GetChecks(ctx *sql.Context) ([]sql.CheckDefinition, error) {
	//TODO: add this
	return nil, nil
}

// Close implements the interface sql.AutoIncrementSetter.
func (t Table) Close(ctx *sql.Context) error {
	return nil
}

// NumRows implements the interface sql.StatisticsTable.
func (t Table) NumRows(ctx *sql.Context) (uint64, error) {
	rows, err := t.db.shim.QueryRows(t.db.name, fmt.Sprintf("SELECT COUNT(*) FROM `%s`;", t.name))
	if err != nil {
		return 0, err
	}
	rowCount, err := sql.Uint64.Convert(rows[0][0])
	if err != nil {
		return 0, err
	}
	return rowCount.(uint64), nil
}

// DataLength implements the interface sql.StatisticsTable.
func (t Table) DataLength(ctx *sql.Context) (uint64, error) {
	// SELECT * FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'sys') AND (TABLE_NAME = 'test');
	rows, err := t.db.shim.QueryRows(t.db.name, fmt.Sprintf("SELECT COUNT(*) FROM `%s`;", t.name))
	if err != nil {
		return 0, err
	}
	rowCount, err := sql.Uint64.Convert(rows[0][0])
	if err != nil {
		return 0, err
	}
	return rowCount.(uint64), nil
}

// CreatePrimaryKey implements the interface sql.PrimaryKeyAlterableTable.
func (t Table) CreatePrimaryKey(ctx *sql.Context, columns []sql.IndexColumn) error {
	pkNames := make([]string, len(columns))
	for i, column := range columns {
		pkNames[i] = column.Name
	}
	return t.db.shim.Exec(t.db.name, fmt.Sprintf("ALTER TABLE `%s` ADD PRIMARY KEY (`%s`);", t.name, strings.Join(pkNames, "`,`")))
}

// DropPrimaryKey implements the interface sql.PrimaryKeyAlterableTable.
func (t Table) DropPrimaryKey(ctx *sql.Context) error {
	return t.db.shim.Exec(t.db.name, fmt.Sprintf("ALTER TABLE `%s` DROP PRIMARY KEY;", t.name))
}

// getCreateTable returns this table as a CreateTable node.
func (t Table) getCreateTable() (*plan.CreateTable, error) {
	rows, err := t.db.shim.QueryRows(t.db.name, fmt.Sprintf("SHOW CREATE TABLE `%s`;", t.name))
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return nil, sql.ErrTableNotFound.New(t.name)
	}
	createTableNode, err := parse.Parse(sql.NewEmptyContext(), rows[0][1].(string))
	if err != nil {
		return nil, err
	}
	return createTableNode.(*plan.CreateTable), nil
}

// randString returns a random string of the given length.
// Retrieved from https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
func randString(n int) string {
	const letterIdxBits = 6
	const letterIdxMask = 1<<letterIdxBits - 1
	const letterIdxMax = 63 / letterIdxBits
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}
