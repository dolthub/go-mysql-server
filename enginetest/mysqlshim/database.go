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
	"io"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

// Database represents a database for a local MySQL server.
type Database struct {
	shim *MySQLShim
	name string
}

var _ sql.Database = Database{}
var _ sql.TableCreator = Database{}
var _ sql.TableDropper = Database{}
var _ sql.TableRenamer = Database{}
var _ sql.TriggerDatabase = Database{}
var _ sql.StoredProcedureDatabase = Database{}
var _ sql.ViewDatabase = Database{}
var _ sql.EventDatabase = Database{}

// Name implements the interface sql.Database.
func (d Database) Name() string {
	return d.name
}

// GetTableInsensitive implements the interface sql.Database.
func (d Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	tables, err := d.GetTableNames(ctx)
	if err != nil {
		return nil, false, err
	}
	lowerName := strings.ToLower(tblName)
	for _, readName := range tables {
		if lowerName == strings.ToLower(readName) {
			return Table{d, readName}, true, nil
		}
	}
	return nil, false, nil
}

// GetTableNames implements the interface sql.Database.
func (d Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	rows, err := d.shim.Query(d.name, "SHOW TABLES;")
	if err != nil {
		return nil, err
	}
	defer rows.Close(ctx)
	var tableNames []string
	var row sql.Row
	for row, err = rows.Next(ctx); err == nil; row, err = rows.Next(ctx) {
		tableNames = append(tableNames, row[0].(string))
	}
	if err != io.EOF {
		return nil, err
	}
	return tableNames, nil
}

// CreateTable implements the interface sql.TableCreator.
func (d Database) CreateTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID) error {
	colStmts := make([]string, len(schema.Schema))
	var primaryKeyCols []string
	for i, col := range schema.Schema {
		stmt := fmt.Sprintf("  `%s` %s", col.Name, col.Type.String())
		if !col.Nullable {
			stmt = fmt.Sprintf("%s NOT NULL", stmt)
		}
		if col.AutoIncrement {
			stmt = fmt.Sprintf("%s AUTO_INCREMENT", stmt)
		}
		if col.Default != nil {
			stmt = fmt.Sprintf("%s DEFAULT %s", stmt, col.Default.String())
		}
		if col.Comment != "" {
			stmt = fmt.Sprintf("%s COMMENT '%s'", stmt, col.Comment)
		}
		if col.PrimaryKey {
			primaryKeyCols = append(primaryKeyCols, col.Name)
		}
		colStmts[i] = stmt
	}
	if len(primaryKeyCols) > 0 {
		primaryKey := fmt.Sprintf("  PRIMARY KEY (`%s`)", strings.Join(primaryKeyCols, "`,`"))
		colStmts = append(colStmts, primaryKey)
	}
	return d.shim.Exec(d.name, fmt.Sprintf("CREATE TABLE `%s` (\n%s\n) ENGINE=InnoDB DEFAULT COLLATE=%s;",
		name, strings.Join(colStmts, ",\n"), sql.Collation_Default.String()))
}

// DropTable implements the interface sql.TableDropper.
func (d Database) DropTable(ctx *sql.Context, name string) error {
	return d.shim.Exec(d.name, fmt.Sprintf("DROP TABLE `%s`;", name))
}

// RenameTable implements the interface sql.TableRenamer.
func (d Database) RenameTable(ctx *sql.Context, oldName, newName string) error {
	return d.shim.Exec(d.name, fmt.Sprintf("RENAME TABLE `%s` TO `%s`;", oldName, newName))
}

// GetTriggers implements the interface sql.TriggerDatabase.
func (d Database) GetTriggers(ctx *sql.Context) ([]sql.TriggerDefinition, error) {
	rows, err := d.shim.Query(d.name, "SHOW TRIGGERS;")
	if err != nil {
		return nil, err
	}
	defer rows.Close(ctx)
	var triggers []sql.TriggerDefinition
	var row sql.Row
	for row, err = rows.Next(ctx); err == nil; row, err = rows.Next(ctx) {
		// Trigger, Event, Table, Statement, Timing, Created, sql_mode, ...
		triggers = append(triggers, sql.TriggerDefinition{
			Name: row[0].(string),
			CreateStatement: fmt.Sprintf("CREATE TRIGGER `%s` %s %s ON `%s` FOR EACH ROW %s;",
				row[0].(string), row[4].(string), row[1].(string), row[2].(string), row[3].(string)),
			CreatedAt: time.Time{}, // TODO: time works in with doltharness
		})
	}
	if err != io.EOF {
		return nil, err
	}
	return triggers, nil
}

// CreateTrigger implements the interface sql.TriggerDatabase.
func (d Database) CreateTrigger(ctx *sql.Context, definition sql.TriggerDefinition) error {
	return d.shim.Exec(d.name, definition.CreateStatement)
}

// DropTrigger implements the interface sql.TriggerDatabase.
func (d Database) DropTrigger(ctx *sql.Context, name string) error {
	return d.shim.Exec(d.name, fmt.Sprintf("DROP TRIGGER `%s`;", name))
}

// GetStoredProcedure implements the interface sql.StoredProcedureDatabase.
func (d Database) GetStoredProcedure(ctx *sql.Context, name string) (sql.StoredProcedureDetails, bool, error) {
	name = strings.ToLower(name)
	procedures, err := d.GetStoredProcedures(ctx)
	if err != nil {
		return sql.StoredProcedureDetails{}, false, err
	}
	for _, procedure := range procedures {
		if name == strings.ToLower(procedure.Name) {
			return procedure, true, nil
		}
	}
	return sql.StoredProcedureDetails{}, false, nil
}

// GetStoredProcedures implements the interface sql.StoredProcedureDatabase.
func (d Database) GetStoredProcedures(ctx *sql.Context) ([]sql.StoredProcedureDetails, error) {
	procedures, err := d.shim.QueryRows("", fmt.Sprintf("SHOW PROCEDURE STATUS WHERE Db = '%s';", d.name))
	if err != nil {
		return nil, err
	}
	storedProcedureDetails := make([]sql.StoredProcedureDetails, len(procedures))
	for i, procedure := range procedures {
		// Db, Name, Type, Definer, Modified, Created, Security_type, Comment, ...
		procedureStatement, err := d.shim.QueryRows("", fmt.Sprintf("SHOW CREATE PROCEDURE `%s`.`%s`;", d.name, procedure[1]))
		if err != nil {
			return nil, err
		}
		// Procedure, sql_mode, Create Procedure, ...
		storedProcedureDetails[i] = sql.StoredProcedureDetails{
			Name:            procedureStatement[0][0].(string),
			CreateStatement: procedureStatement[0][2].(string),
			CreatedAt:       time.Time{}, // these should be added someday
			ModifiedAt:      time.Time{},
		}
	}
	return storedProcedureDetails, nil
}

// SaveStoredProcedure implements the interface sql.StoredProcedureDatabase.
func (d Database) SaveStoredProcedure(ctx *sql.Context, spd sql.StoredProcedureDetails) error {
	return d.shim.Exec(d.name, spd.CreateStatement)
}

// DropStoredProcedure implements the interface sql.StoredProcedureDatabase.
func (d Database) DropStoredProcedure(ctx *sql.Context, name string) error {
	return d.shim.Exec(d.name, fmt.Sprintf("DROP PROCEDURE `%s`;", name))
}

// GetEvent implements sql.EventDatabase
func (d Database) GetEvent(ctx *sql.Context, name string) (sql.EventDefinition, bool, error) {
	name = strings.ToLower(name)
	events, _, err := d.GetEvents(ctx)
	if err != nil {
		return sql.EventDefinition{}, false, err
	}
	for _, event := range events {
		if name == strings.ToLower(event.Name) {
			return event, true, nil
		}
	}
	return sql.EventDefinition{}, false, nil
}

// GetEvents implements sql.EventDatabase
func (d Database) GetEvents(_ *sql.Context) ([]sql.EventDefinition, interface{}, error) {
	events, err := d.shim.QueryRows("", fmt.Sprintf("SHOW EVENTS WHERE Db = '%s';", d.name))
	if err != nil {
		return nil, nil, err
	}
	eventDefinition := make([]sql.EventDefinition, len(events))
	for i, event := range events {
		// Db, Name, Definer, Time Zone, Type, ...
		eventStmt, err := d.shim.QueryRows("", fmt.Sprintf("SHOW CREATE EVENT `%s`.`%s`;", d.name, event[1]))
		if err != nil {
			return nil, nil, err
		}
		// Event, sql_mode, time_zone, Create Event, ...
		eventDefinition[i] = sql.EventDefinition{
			Name: eventStmt[0][0].(string),
			// TODO: other fields should be added such as Created, LastAltered
		}
	}
	// MySQL shim doesn't support event reloading, so token is always nil
	return eventDefinition, nil, nil
}

// SaveEvent implements sql.EventDatabase
func (d Database) SaveEvent(ctx *sql.Context, event sql.EventDefinition) (bool, error) {
	return event.Status == sql.EventStatus_Enable.String(), d.shim.Exec(d.name, event.CreateEventStatement())
}

// DropEvent implements sql.EventDatabase
func (d Database) DropEvent(ctx *sql.Context, name string) error {
	return d.shim.Exec(d.name, fmt.Sprintf("DROP EVENT `%s`;", name))
}

// UpdateEvent implements sql.EventDatabase
func (d Database) UpdateEvent(_ *sql.Context, originalName string, event sql.EventDefinition) (bool, error) {
	err := d.shim.Exec(d.name, fmt.Sprintf("DROP EVENT `%s`;", originalName))
	if err != nil {
		return false, err
	}
	return event.Status == sql.EventStatus_Enable.String(), d.shim.Exec(d.name, event.CreateEventStatement())
}

// NeedsToReloadEvents implements sql.EventDatabase
func (d Database) NeedsToReloadEvents(_ *sql.Context, _ interface{}) (bool, error) {
	// mysqlshim does not support event reloading
	return false, nil
}

// UpdateLastExecuted implements sql.EventDatabase
func (d Database) UpdateLastExecuted(ctx *sql.Context, eventName string, lastExecuted time.Time) error {
	return nil
}

// CreateView implements the interface sql.ViewDatabase.
func (d Database) CreateView(ctx *sql.Context, name string, selectStatement, createViewStmt string) error {
	return d.shim.Exec(d.name, createViewStmt)
}

// DropView implements the interface sql.ViewDatabase.
func (d Database) DropView(ctx *sql.Context, name string) error {
	return d.shim.Exec(d.name, fmt.Sprintf("DROP VIEW `%s`;", name))
}

// GetViewDefinition implements the interface sql.ViewDatabase.
func (d Database) GetViewDefinition(ctx *sql.Context, viewName string) (sql.ViewDefinition, bool, error) {
	views, err := d.AllViews(ctx)
	if err != nil {
		return sql.ViewDefinition{}, false, err
	}
	lowerName := strings.ToLower(viewName)
	for _, view := range views {
		if lowerName == strings.ToLower(view.Name) {
			return view, true, nil
		}
	}
	return sql.ViewDefinition{}, false, nil
}

// AllViews implements the interface sql.ViewDatabase.
func (d Database) AllViews(ctx *sql.Context) ([]sql.ViewDefinition, error) {
	views, err := d.shim.QueryRows("", fmt.Sprintf("SELECT * FROM information_schema.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_TYPE = 'VIEW';", d.name))
	if err != nil {
		return nil, err
	}
	viewDefinitions := make([]sql.ViewDefinition, len(views))
	for i, view := range views {
		viewName := view[2].(string)
		viewStatementRow, err := d.shim.QueryRows("", fmt.Sprintf("SHOW CREATE VIEW `%s`.`%s`;", d.name, viewName))
		if err != nil {
			return nil, err
		}
		createViewStatement := viewStatementRow[0][1].(string)
		viewStatement := createViewStatement[strings.Index(createViewStatement, " AS ")+4:] // not the best but works for now
		viewDefinitions[i] = sql.ViewDefinition{
			Name:                viewName,
			TextDefinition:      viewStatement,
			CreateViewStatement: createViewStatement,
		}
	}
	return viewDefinitions, nil
}
