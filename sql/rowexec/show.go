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

package rowexec

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	gmstime "github.com/dolthub/go-mysql-server/internal/time"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *BaseBuilder) buildShowCharset(ctx *sql.Context, n *plan.ShowCharset, row sql.Row) (sql.RowIter, error) {
	//TODO: use the information_schema table instead, currently bypassing it to show currently-implemented charsets
	//ri, err := sc.CharacterSetTable.RowIter(ctx, row)
	//if err != nil {
	//	return nil, err
	//}
	//return &showCharsetIter{originalIter: ri}, nil

	var rows []sql.Row
	iter := sql.NewCharacterSetsIterator()
	for charset, ok := iter.Next(); ok; charset, ok = iter.Next() {
		if charset.Encoder != nil && charset.BinaryCollation.Sorter() != nil && charset.DefaultCollation.Sorter() != nil {
			rows = append(rows, sql.Row{
				charset.Name,
				charset.Description,
				charset.DefaultCollation.String(),
				uint64(charset.MaxLength),
			})
		}
	}
	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildDescribeQuery(ctx *sql.Context, n *plan.DescribeQuery, row sql.Row) (sql.RowIter, error) {
	var rows []sql.Row
	var formatString string
	if n.Format == "debug" {
		formatString = sql.DebugString(n.Child)
	} else {
		formatString = n.Child.String()
	}

	for _, l := range strings.Split(formatString, "\n") {
		if strings.TrimSpace(l) != "" {
			rows = append(rows, sql.NewRow(l))
		}
	}
	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildShowWarnings(ctx *sql.Context, n plan.ShowWarnings, row sql.Row) (sql.RowIter, error) {
	var rows []sql.Row
	for _, w := range n {
		rows = append(rows, sql.NewRow(w.Level, w.Code, w.Message))
	}
	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildShowProcessList(ctx *sql.Context, n *plan.ShowProcessList, row sql.Row) (sql.RowIter, error) {
	processes := ctx.ProcessList.Processes()
	var rows = make([]sql.Row, len(processes))

	for i, proc := range processes {
		var status []string
		var names []string
		for name := range proc.Progress {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, name := range names {
			progress := proc.Progress[name]

			printer := sql.NewTreePrinter()
			_ = printer.WriteNode("\n" + progress.String())
			children := []string{}
			for _, partitionProgress := range progress.PartitionsProgress {
				children = append(children, partitionProgress.String())
			}
			sort.Strings(children)
			_ = printer.WriteChildren(children...)

			status = append(status, printer.String())
		}

		if len(status) == 0 && proc.Command == sql.ProcessCommandQuery {
			status = []string{"running"}
		}

		rows[i] = process{
			id:      int64(proc.Connection),
			user:    proc.User,
			time:    int64(proc.Seconds()),
			state:   strings.Join(status, ""),
			command: string(proc.Command),
			host:    proc.Host,
			info:    proc.Query,
			db:      proc.Database,
		}.toRow()
	}

	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildShowTableStatus(ctx *sql.Context, n *plan.ShowTableStatus, row sql.Row) (sql.RowIter, error) {
	tables, err := n.Database().GetTableNames(ctx)
	if err != nil {
		return nil, err
	}

	var rows = make([]sql.Row, len(tables))

	for i, tName := range tables {
		table, _, err := n.Catalog.Table(ctx, n.Database().Name(), tName)
		if err != nil {
			return nil, err
		}

		var numRows uint64
		var dataLength uint64

		if st, ok := table.(sql.StatisticsTable); ok {
			numRows, _, err = st.RowCount(ctx)
			if err != nil {
				return nil, err
			}

			dataLength, err = st.DataLength(ctx)
			if err != nil {
				return nil, err
			}
		}

		rows[i] = tableToStatusRow(tName, numRows, dataLength, table.Collation())
	}

	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildShowTables(ctx *sql.Context, n *plan.ShowTables, row sql.Row) (sql.RowIter, error) {
	var tableNames []string

	// TODO: this entire analysis should really happen in the analyzer, as opposed to at execution time
	if n.AsOf() != nil {
		if vdb, ok := n.Database().(sql.VersionedDatabase); ok {
			asOf, err := n.AsOf().Eval(ctx, nil)
			if err != nil {
				return nil, err
			}

			tableNames, err = vdb.GetTableNamesAsOf(ctx, asOf)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, sql.ErrAsOfNotSupported.New(n.Database().Name())
		}
	} else {
		var err error
		tableNames, err = n.Database().GetTableNames(ctx)
		if err != nil {
			return nil, err
		}
	}

	sort.Strings(tableNames)

	var rows []sql.Row
	for _, tableName := range tableNames {
		row := sql.Row{tableName}
		if n.Full {
			row = append(row, "BASE TABLE")
		}
		rows = append(rows, row)
	}

	// TODO: currently there is no way to see views AS OF a particular time
	db := n.Database()
	if vdb, ok := db.(sql.ViewDatabase); ok {
		views, err := vdb.AllViews(ctx)
		if err != nil {
			return nil, err
		}
		for _, view := range views {
			row := sql.Row{view.Name}
			if n.Full {
				row = append(row, "VIEW")
			}
			rows = append(rows, row)
		}
	}

	for _, view := range ctx.GetViewRegistry().ViewsInDatabase(db.Name()) {
		row := sql.Row{view.Name()}
		if n.Full {
			row = append(row, "VIEW")
		}
		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0].(string) < rows[j][0].(string)
	})

	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildShowStatus(ctx *sql.Context, n *plan.ShowStatus, row sql.Row) (sql.RowIter, error) {
	var names []string
	for name := range sql.SystemVariables.NewSessionMap() {
		names = append(names, name)
	}
	sort.Strings(names)

	var rows []sql.Row
	for _, name := range names {
		sysVar, val, ok := sql.SystemVariables.GetGlobal(name)
		if !ok {
			return nil, fmt.Errorf("missing system variable %s", name)
		}

		if n.Modifier == plan.ShowStatusModifier_Session && sysVar.Scope == sql.SystemVariableScope_Global ||
			n.Modifier == plan.ShowStatusModifier_Global && sysVar.Scope == sql.SystemVariableScope_Session {
			continue
		}

		rows = append(rows, sql.Row{name, val})
	}

	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildShowCreateProcedure(ctx *sql.Context, n *plan.ShowCreateProcedure, row sql.Row) (sql.RowIter, error) {
	characterSetClient, err := ctx.GetSessionVariable(ctx, "character_set_client")
	if err != nil {
		return nil, err
	}
	collationConnection, err := ctx.GetSessionVariable(ctx, "collation_connection")
	if err != nil {
		return nil, err
	}
	collationServer, err := ctx.GetSessionVariable(ctx, "collation_server")
	if err != nil {
		return nil, err
	}

	if n.ExternalStoredProcedure != nil {
		// If an external stored procedure has been plugged in by the analyzer, use that
		fakeCreateProcedureStmt := n.ExternalStoredProcedure.FakeCreateProcedureStmt()
		return sql.RowsToRowIter(sql.Row{
			n.ExternalStoredProcedure.Name, // Procedure
			"",                             // sql_mode
			fakeCreateProcedureStmt,        // Create Procedure
			characterSetClient,             // character_set_client
			collationConnection,            // collation_connection
			collationServer,                // Database Collation
		}), nil
	} else {
		// Otherwise, search the StoredProcedureDatabase for a user-created stored procedure
		procedureDb, ok := n.Database().(sql.StoredProcedureDatabase)
		if !ok {
			return nil, sql.ErrStoredProceduresNotSupported.New(n.Database().Name())
		}
		procedures, err := procedureDb.GetStoredProcedures(ctx)
		if err != nil {
			return nil, err
		}
		for _, procedure := range procedures {
			if strings.ToLower(procedure.Name) == n.ProcedureName {
				return sql.RowsToRowIter(sql.Row{
					procedure.Name,            // Procedure
					"",                        // sql_mode
					procedure.CreateStatement, // Create Procedure
					characterSetClient,        // character_set_client
					collationConnection,       // collation_connection
					collationServer,           // Database Collation
				}), nil
			}
		}
		return nil, sql.ErrStoredProcedureDoesNotExist.New(n.ProcedureName)
	}
}

func (b *BaseBuilder) buildShowCreateDatabase(ctx *sql.Context, n *plan.ShowCreateDatabase, row sql.Row) (sql.RowIter, error) {
	var name = n.Database().Name()

	var buf bytes.Buffer

	buf.WriteString("CREATE DATABASE ")
	if n.IfNotExists {
		buf.WriteString("/*!32312 IF NOT EXISTS*/ ")
	}

	buf.WriteRune('`')
	buf.WriteString(name)
	buf.WriteRune('`')
	buf.WriteString(fmt.Sprintf(
		" /*!40100 DEFAULT CHARACTER SET %s COLLATE %s */",
		sql.Collation_Default.CharacterSet().String(),
		sql.Collation_Default.String(),
	))

	return sql.RowsToRowIter(
		sql.NewRow(name, buf.String()),
	), nil
}

func (b *BaseBuilder) buildShowPrivileges(ctx *sql.Context, n *plan.ShowPrivileges, row sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(
		sql.Row{"Alter", "Tables", "To alter the table"},
		sql.Row{"Alter routine", "Functions,Procedures", "To alter or drop stored functions/procedures"},
		sql.Row{"Create", "Databases,Tables,Indexes", "To create new databases and tables"},
		sql.Row{"Create routine", "Databases", "To use CREATE FUNCTION/PROCEDURE"},
		sql.Row{"Create role", "Server Admin", "To create new roles"},
		sql.Row{"Create temporary tables", "Databases", "To use CREATE TEMPORARY TABLE"},
		sql.Row{"Create view", "Tables", "To create new views"},
		sql.Row{"Create user", "Server Admin", "To create new users"},
		sql.Row{"Delete", "Tables", "To delete existing rows"},
		sql.Row{"Drop", "Databases,Tables", "To drop databases, tables, and views"},
		sql.Row{"Drop role", "Server Admin", "To drop roles"},
		sql.Row{"Event", "Server Admin", "To create, alter, drop and execute events"},
		sql.Row{"Execute", "Functions,Procedures", "To execute stored routines"},
		sql.Row{"File", "File access on server", "To read and write files on the server"},
		sql.Row{"Grant option", "Databases,Tables,Functions,Procedures", "To give to other users those privileges you possess"},
		sql.Row{"Index", "Tables", "To create or drop indexes"},
		sql.Row{"Insert", "Tables", "To insert data into tables"},
		sql.Row{"Lock tables", "Databases", "To use LOCK TABLES (together with SELECT privilege)"},
		sql.Row{"Process", "Server Admin", "To view the plain text of currently executing queries"},
		sql.Row{"Proxy", "Server Admin", "To make proxy user possible"},
		sql.Row{"References", "Databases,Tables", "To have references on tables"},
		sql.Row{"Reload", "Server Admin", "To reload or refresh tables, logs and privileges"},
		sql.Row{"Replication client", "Server Admin", "To ask where the slave or master servers are"},
		sql.Row{"Replication slave", "Server Admin", "To read binary log events from the master"},
		sql.Row{"Select", "Tables", "To retrieve rows from table"},
		sql.Row{"Show databases", "Server Admin", "To see all databases with SHOW DATABASES"},
		sql.Row{"Show view", "Tables", "To see views with SHOW CREATE VIEW"},
		sql.Row{"Shutdown", "Server Admin", "To shut down the server"},
		sql.Row{"Super", "Server Admin", "To use KILL thread, SET GLOBAL, CHANGE MASTER, etc."},
		sql.Row{"Trigger", "Tables", "To use triggers"},
		sql.Row{"Create tablespace", "Server Admin", "To create/alter/drop tablespaces"},
		sql.Row{"Update", "Tables", "To update existing rows"},
		sql.Row{"Usage", "Server Admin", "No privileges - allow connect only"},
		sql.Row{"ENCRYPTION_KEY_ADMIN", "Server Admin", ""},
		sql.Row{"INNODB_REDO_LOG_ARCHIVE", "Server Admin", ""},
		sql.Row{"REPLICATION_APPLIER", "Server Admin", ""},
		sql.Row{"INNODB_REDO_LOG_ENABLE", "Server Admin", ""},
		sql.Row{"SET_USER_ID", "Server Admin", ""},
		sql.Row{"SERVICE_CONNECTION_ADMIN", "Server Admin", ""},
		sql.Row{"GROUP_REPLICATION_ADMIN", "Server Admin", ""},
		sql.Row{"AUDIT_ABORT_EXEMPT", "Server Admin", ""},
		sql.Row{"GROUP_REPLICATION_STREAM", "Server Admin", ""},
		sql.Row{"CLONE_ADMIN", "Server Admin", ""},
		sql.Row{"SYSTEM_USER", "Server Admin", ""},
		sql.Row{"AUTHENTICATION_POLICY_ADMIN", "Server Admin", ""},
		sql.Row{"SHOW_ROUTINE", "Server Admin", ""},
		sql.Row{"BACKUP_ADMIN", "Server Admin", ""},
		sql.Row{"CONNECTION_ADMIN", "Server Admin", ""},
		sql.Row{"PERSIST_RO_VARIABLES_ADMIN", "Server Admin", ""},
		sql.Row{"RESOURCE_GROUP_ADMIN", "Server Admin", ""},
		sql.Row{"SESSION_VARIABLES_ADMIN", "Server Admin", ""},
		sql.Row{"SYSTEM_VARIABLES_ADMIN", "Server Admin", ""},
		sql.Row{"APPLICATION_PASSWORD_ADMIN", "Server Admin", ""},
		sql.Row{"FLUSH_OPTIMIZER_COSTS", "Server Admin", ""},
		sql.Row{"AUDIT_ADMIN", "Server Admin", ""},
		sql.Row{"BINLOG_ADMIN", "Server Admin", ""},
		sql.Row{"BINLOG_ENCRYPTION_ADMIN", "Server Admin", ""},
		sql.Row{"FLUSH_STATUS", "Server Admin", ""},
		sql.Row{"FLUSH_TABLES", "Server Admin", ""},
		sql.Row{"FLUSH_USER_RESOURCES", "Server Admin", ""},
		sql.Row{"XA_RECOVER_ADMIN", "Server Admin", ""},
		sql.Row{"PASSWORDLESS_USER_ADMIN", "Server Admin", ""},
		sql.Row{"TABLE_ENCRYPTION_ADMIN", "Server Admin", ""},
		sql.Row{"ROLE_ADMIN", "Server Admin", ""},
		sql.Row{"REPLICATION_SLAVE_ADMIN", "Server Admin", ""},
		sql.Row{"RESOURCE_GROUP_USER", "Server Admin", ""},
	), nil
}

func (b *BaseBuilder) buildShowCreateTrigger(ctx *sql.Context, n *plan.ShowCreateTrigger, row sql.Row) (sql.RowIter, error) {
	triggerDb, ok := n.Database().(sql.TriggerDatabase)
	if !ok {
		return nil, sql.ErrTriggersNotSupported.New(n.Database().Name())
	}
	triggers, err := triggerDb.GetTriggers(ctx)
	if err != nil {
		return nil, err
	}
	for _, trigger := range triggers {
		if strings.ToLower(trigger.Name) == n.TriggerName {
			characterSetClient, err := ctx.GetSessionVariable(ctx, "character_set_client")
			if err != nil {
				return nil, err
			}
			collationConnection, err := ctx.GetSessionVariable(ctx, "collation_connection")
			if err != nil {
				return nil, err
			}
			collationServer, err := ctx.GetSessionVariable(ctx, "collation_server")
			if err != nil {
				return nil, err
			}
			return sql.RowsToRowIter(sql.Row{
				trigger.Name,            // Trigger
				"",                      // sql_mode
				trigger.CreateStatement, // SQL Original Statement
				characterSetClient,      // character_set_client
				collationConnection,     // collation_connection
				collationServer,         // Database Collation
				trigger.CreatedAt,       // Created
			}), nil
		}
	}
	return nil, sql.ErrTriggerDoesNotExist.New(n.TriggerName)
}

func (b *BaseBuilder) buildShowColumns(ctx *sql.Context, n *plan.ShowColumns, row sql.Row) (sql.RowIter, error) {
	span, _ := ctx.Span("plan.ShowColumns")

	schema := n.TargetSchema()
	var rows = make([]sql.Row, len(schema))
	for i, col := range schema {
		var row sql.Row
		var collation interface{}
		if types.IsTextOnly(col.Type) {
			collation = sql.Collation_Default.String()
		}

		var null = "NO"
		if col.Nullable {
			null = "YES"
		}

		node := n.Child
		if exchange, ok := node.(*plan.Exchange); ok {
			node = exchange.Child
		}
		key := ""
		switch table := node.(type) {
		case *plan.ResolvedTable:
			if col.PrimaryKey {
				key = "PRI"
			} else if isFirstColInUniqueKey(n, col, table) {
				key = "UNI"
			} else if isFirstColInNonUniqueKey(n, col, table) {
				key = "MUL"
			}
		case *plan.SubqueryAlias:
			// no key info for views
		default:
			panic(fmt.Sprintf("unexpected type %T", n.Child))
		}

		var defaultVal string
		if col.Default != nil {
			defaultVal = col.Default.String()
		} else {
			// From: https://dev.mysql.com/doc/refman/8.0/en/show-columns.html
			// The default value for the column. This is NULL if the column has an explicit default of NULL,
			// or if the column definition includes no DEFAULT clause.
			defaultVal = "NULL"
		}

		extra := col.Extra
		// If extra is not defined, fill it here.
		if extra == "" && !col.Default.IsLiteral() {
			extra = "DEFAULT_GENERATED"
		}

		if n.Full {
			row = sql.Row{
				col.Name,
				col.Type.String(),
				collation,
				null,
				key,
				defaultVal,
				extra,
				"", // Privileges
				col.Comment,
			}
		} else {
			row = sql.Row{
				col.Name,
				col.Type.String(),
				null,
				key,
				defaultVal,
				extra,
			}
		}

		rows[i] = row
	}

	return sql.NewSpanIter(span, sql.RowsToRowIter(rows...)), nil
}

func (b *BaseBuilder) buildShowVariables(ctx *sql.Context, n *plan.ShowVariables, row sql.Row) (sql.RowIter, error) {
	var rows []sql.Row
	var sysVars map[string]interface{}

	if n.Global {
		sysVars = sql.SystemVariables.GetAllGlobalVariables()
	} else {
		sysVars = ctx.GetAllSessionVariables()
	}

	for k, v := range sysVars {
		if n.Filter != nil {
			res, err := n.Filter.Eval(ctx, sql.Row{k})
			if err != nil {
				return nil, err
			}
			res, _, err = types.Boolean.Convert(res)
			if err != nil {
				ctx.Warn(1292, err.Error())
				continue
			}
			if res.(int8) == 0 {
				continue
			}
		}
		rows = append(rows, sql.NewRow(k, v))
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0].(string) < rows[j][0].(string)
	})

	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildShowTriggers(ctx *sql.Context, n *plan.ShowTriggers, row sql.Row) (sql.RowIter, error) {
	var rows []sql.Row
	for _, trigger := range n.Triggers {
		triggerEvent := strings.ToUpper(trigger.TriggerEvent)
		triggerTime := strings.ToUpper(trigger.TriggerTime)
		tableName := trigger.Table.(sql.Nameable).Name()
		characterSetClient, err := ctx.GetSessionVariable(ctx, "character_set_client")
		if err != nil {
			return nil, err
		}
		collationConnection, err := ctx.GetSessionVariable(ctx, "collation_connection")
		if err != nil {
			return nil, err
		}
		collationServer, err := ctx.GetSessionVariable(ctx, "collation_server")
		if err != nil {
			return nil, err
		}
		rows = append(rows, sql.Row{
			trigger.TriggerName, // Trigger
			triggerEvent,        // Event
			tableName,           // Table
			trigger.BodyString,  // Statement
			triggerTime,         // Timing
			trigger.CreatedAt,   // Created
			"",                  // sql_mode
			"",                  // Definer
			characterSetClient,  // character_set_client
			collationConnection, // collation_connection
			collationServer,     // Database Collation
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildDescribe(ctx *sql.Context, n *plan.Describe, row sql.Row) (sql.RowIter, error) {
	return &describeIter{schema: n.Child.Schema()}, nil
}

func (b *BaseBuilder) buildShowDatabases(ctx *sql.Context, n *plan.ShowDatabases, row sql.Row) (sql.RowIter, error) {
	dbs := n.Catalog.AllDatabases(ctx)
	var rows = make([]sql.Row, 0, len(dbs))
	for _, db := range dbs {
		rows = append(rows, sql.Row{db.Name()})
	}
	if _, err := n.Catalog.Database(ctx, "mysql"); err == nil {
		rows = append(rows, sql.Row{"mysql"})
	}

	sort.Slice(rows, func(i, j int) bool {
		return strings.Compare(rows[i][0].(string), rows[j][0].(string)) < 0
	})

	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildShowGrants(ctx *sql.Context, n *plan.ShowGrants, row sql.Row) (sql.RowIter, error) {
	mysqlDb, ok := n.MySQLDb.(*mysql_db.MySQLDb)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	if n.For == nil || n.CurrentUser {
		client := ctx.Session.Client()
		n.For = &plan.UserName{
			Name: client.User,
			Host: client.Address,
		}
	}

	reader := mysqlDb.Reader()
	defer reader.Close()

	user := mysqlDb.GetUser(reader, n.For.Name, n.For.Host, false)
	if user == nil {
		return nil, sql.ErrShowGrantsUserDoesNotExist.New(n.For.Name, n.For.Host)
	}

	//TODO: implement USING, perhaps by creating a new context with the chosen roles set as the active roles
	var rows []sql.Row
	userStr := user.UserHostToString("`")
	privStr := generatePrivStrings("*", "*", userStr, user.PrivilegeSet.ToSlice())
	rows = append(rows, sql.Row{privStr})

	for _, db := range user.PrivilegeSet.GetDatabases() {
		dbStr := fmt.Sprintf("`%s`", db.Name())
		if privStr = generatePrivStrings(dbStr, "*", userStr, db.ToSlice()); len(privStr) != 0 {
			rows = append(rows, sql.Row{privStr})
		}

		for _, tbl := range db.GetTables() {
			tblStr := fmt.Sprintf("`%s`", tbl.Name())
			privStr = generatePrivStrings(dbStr, tblStr, userStr, tbl.ToSlice())
			rows = append(rows, sql.Row{privStr})
		}
	}

	// TODO: display column privileges

	sb := strings.Builder{}

	roleEdges := reader.GetToUserRoleEdges(mysql_db.RoleEdgesToKey{
		ToHost: user.Host,
		ToUser: user.User,
	})
	for i, roleEdge := range roleEdges {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(roleEdge.FromString("`"))
	}
	if sb.Len() > 0 {
		rows = append(rows, sql.Row{fmt.Sprintf("GRANT %s TO %s", sb.String(), user.UserHostToString("`"))})
	}

	sb.Reset()
	for i, dynamicPrivWithWgo := range user.PrivilegeSet.ToSliceDynamic(true) {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(dynamicPrivWithWgo)
	}
	if sb.Len() > 0 {
		rows = append(rows, sql.Row{fmt.Sprintf("GRANT %s ON *.* TO %s WITH GRANT OPTION", sb.String(), user.UserHostToString("`"))})
	}
	sb.Reset()
	for i, dynamicPrivWithoutWgo := range user.PrivilegeSet.ToSliceDynamic(false) {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(dynamicPrivWithoutWgo)
	}
	if sb.Len() > 0 {
		rows = append(rows, sql.Row{fmt.Sprintf("GRANT %s ON *.* TO %s", sb.String(), user.UserHostToString("`"))})
	}
	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildShowIndexes(ctx *sql.Context, n *plan.ShowIndexes, row sql.Row) (sql.RowIter, error) {
	table, ok := n.Child.(*plan.ResolvedTable)
	if !ok {
		panic(fmt.Sprintf("unexpected type %T", n.Child))
	}

	return &showIndexesIter{
		table: table,
		idxs:  newIndexesToShow(n.IndexesToShow),
	}, nil
}

func (b *BaseBuilder) buildShowCreateTable(ctx *sql.Context, n *plan.ShowCreateTable, row sql.Row) (sql.RowIter, error) {
	return &showCreateTablesIter{
		table:    n.Child,
		isView:   n.IsView,
		indexes:  n.Indexes,
		checks:   n.Checks(),
		schema:   n.TargetSchema(),
		pkSchema: n.PrimaryKeySchema,
	}, nil
}

func (b *BaseBuilder) buildShowReplicaStatus(ctx *sql.Context, n *plan.ShowReplicaStatus, row sql.Row) (sql.RowIter, error) {
	if n.ReplicaController == nil {
		return sql.RowsToRowIter(), nil
	}

	status, err := n.ReplicaController.GetReplicaStatus(ctx)
	if err != nil {
		return nil, err
	}
	if status == nil {
		return sql.RowsToRowIter(), nil
	}

	replicateDoTables := strings.Join(status.ReplicateDoTables, ",")
	replicateIgnoreTables := strings.Join(status.ReplicateIgnoreTables, ",")

	lastIoErrorTimestamp := formatReplicaStatusTimestamp(status.LastIoErrorTimestamp)
	lastSqlErrorTimestamp := formatReplicaStatusTimestamp(status.LastSqlErrorTimestamp)

	row = sql.Row{
		"",                       // Replica_IO_State
		status.SourceHost,        // Source_Host
		status.SourceUser,        // Source_User
		status.SourcePort,        // Source_Port
		status.ConnectRetry,      // Connect_Retry
		"INVALID",                // Source_Log_File
		0,                        // Read_Source_Log_Pos
		nil,                      // Relay_Log_File
		nil,                      // Relay_Log_Pos
		"INVALID",                // Relay_Source_Log_File
		status.ReplicaIoRunning,  // Replica_IO_Running
		status.ReplicaSqlRunning, // Replica_SQL_Running
		nil,                      // Replicate_Do_DB
		nil,                      // Replicate_Ignore_DB
		replicateDoTables,        // Replicate_Do_Table
		replicateIgnoreTables,    // Replicate_Ignore_Table
		nil,                      // Replicate_Wild_Do_Table
		nil,                      // Replicate_Wild_Ignore_Table
		status.LastSqlErrNumber,  // Last_Errno
		status.LastSqlError,      // Last_Error
		nil,                      // Skip_Counter
		0,                        // Exec_Source_Log_Pos
		nil,                      // Relay_Log_Space
		"None",                   // Until_Condition
		nil,                      // Until_Log_File
		nil,                      // Until_Log_Pos
		"Ignored",                // Source_SSL_Allowed
		nil,                      // Source_SSL_CA_File
		nil,                      // Source_SSL_CA_Path
		nil,                      // Source_SSL_Cert
		nil,                      // Source_SSL_Cipher
		nil,                      // Source_SSL_CRL_File
		nil,                      // Source_SSL_CRL_Path
		nil,                      // Source_SSL_Key
		nil,                      // Source_SSL_Verify_Server_Cert
		0,                        // Seconds_Behind_Source
		status.LastIoErrNumber,   // Last_IO_Errno
		status.LastIoError,       // Last_IO_Error
		status.LastSqlErrNumber,  // Last_SQL_Errno
		status.LastSqlError,      // Last_SQL_Error
		nil,                      // Replicate_Ignore_Server_Ids
		status.SourceServerId,    // Source_Server_Id
		status.SourceServerUuid,  // Source_UUID
		nil,                      // Source_Info_File
		0,                        // SQL_Delay
		0,                        // SQL_Remaining_Delay
		nil,                      // Replica_SQL_Running_State
		status.SourceRetryCount,  // Source_Retry_Count
		nil,                      // Source_Bind
		lastIoErrorTimestamp,     // Last_IO_Error_Timestamp
		lastSqlErrorTimestamp,    // Last_SQL_Error_Timestamp
		status.RetrievedGtidSet,  // Retrieved_Gtid_Set
		status.ExecutedGtidSet,   // Executed_Gtid_Set
		status.AutoPosition,      // Auto_Position
		nil,                      // Replicate_Rewrite_DB
	}

	return sql.RowsToRowIter(row), nil
}

func (b *BaseBuilder) buildShowCreateEvent(ctx *sql.Context, n *plan.ShowCreateEvent, row sql.Row) (sql.RowIter, error) {
	characterSetClient, err := ctx.GetSessionVariable(ctx, "character_set_client")
	if err != nil {
		return nil, err
	}
	collationConnection, err := ctx.GetSessionVariable(ctx, "collation_connection")
	if err != nil {
		return nil, err
	}
	collationServer, err := ctx.GetSessionVariable(ctx, "collation_server")
	if err != nil {
		return nil, err
	}

	// Convert the Event's timestamps into the session's timezone (they are always stored in UTC)
	newEvent := n.Event.ConvertTimesFromUTCToTz(gmstime.SystemTimezoneOffset())
	n.Event = *newEvent

	// TODO: fill time_zone with appropriate values
	return sql.RowsToRowIter(sql.Row{
		n.Event.Name,                   // Event
		n.Event.SqlMode,                // sql_mode
		"SYSTEM",                       // time_zone
		n.Event.CreateEventStatement(), // Create Event
		characterSetClient,             // character_set_client
		collationConnection,            // collation_connection
		collationServer,                // Database Collation
	}), nil
}
