// Copyright 2020-2021 Dolthub, Inc.
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

package parse

import (
	"encoding/hex"
	goerrors "errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/encodings"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var (
	errIncorrectIndexName = errors.NewKind("incorrect index name '%s'")

	errInvalidDescribeFormat = errors.NewKind("invalid format %q for DESCRIBE, supported formats: %s")

	errInvalidSortOrder = errors.NewKind("invalid sort order: %s")

	ErrPrimaryKeyOnNullField = errors.NewKind("All parts of PRIMARY KEY must be NOT NULL")

	tableCharsetOptionRegex = regexp.MustCompile(`(?i)(DEFAULT)?\s+CHARACTER\s+SET((\s*=?\s*)|\s+)([A-Za-z0-9_]+)`)

	tableCollationOptionRegex = regexp.MustCompile(`(?i)(DEFAULT)?\s+COLLATE((\s*=?\s*)|\s+)([A-Za-z0-9_]+)`)
)

var describeSupportedFormats = []string{"tree"}

// These constants aren't exported from vitess for some reason. This could be removed if we changed this.
const (
	colKeyNone sqlparser.ColumnKeyOption = iota
	colKeyPrimary
	colKeySpatialKey
	colKeyUnique
	colKeyUniqueKey
	colKey
	colKeyFulltextKey
)

// Parse parses the given SQL sentence and returns the corresponding node.
func Parse(ctx *sql.Context, query string) (sql.Node, error) {
	n, _, _, err := parse(ctx, query, false)
	return n, err
}

func ParseOne(ctx *sql.Context, query string) (sql.Node, string, string, error) {
	return parse(ctx, query, true)
}

func parse(ctx *sql.Context, query string, multi bool) (sql.Node, string, string, error) {
	span, ctx := ctx.Span("parse", trace.WithAttributes(attribute.String("query", query)))
	defer span.End()

	s := strings.TrimSpace(query)
	// trim spaces and empty statements
	s = strings.TrimRightFunc(s, func(r rune) bool {
		return r == ';' || unicode.IsSpace(r)
	})

	var stmt sqlparser.Statement
	var err error
	var parsed string
	var remainder string

	parsed = s
	if !multi {
		stmt, err = sqlparser.Parse(s)
	} else {
		var ri int
		stmt, ri, err = sqlparser.ParseOne(s)
		if ri != 0 && ri < len(s) {
			parsed = s[:ri]
			parsed = strings.TrimSpace(parsed)
			// trim spaces and empty statements
			parsed = strings.TrimRightFunc(parsed, func(r rune) bool {
				return r == ';' || unicode.IsSpace(r)
			})
			remainder = s[ri:]
		}
	}

	if err != nil {
		if goerrors.Is(err, sqlparser.ErrEmpty) {
			ctx.Warn(0, "query was empty after trimming comments, so it will be ignored")
			return plan.NothingImpl, parsed, remainder, nil
		}
		return nil, parsed, remainder, sql.ErrSyntaxError.New(err.Error())
	}

	node, err := convert(ctx, stmt, s)

	return node, parsed, remainder, err
}

// ParseColumnTypeString will return a SQL type for the given string that represents a column type.
// For example, giving the string `VARCHAR(255)` will return the string SQL type with the internal type set to Varchar
// and the length set to 255 with the default collation.
func ParseColumnTypeString(ctx *sql.Context, columnType string) (sql.Type, error) {
	createStmt := fmt.Sprintf("CREATE TABLE a(b %s)", columnType)
	parseResult, err := sqlparser.Parse(createStmt)
	if err != nil {
		return nil, err
	}
	node, err := convertDDL(ctx, createStmt, parseResult.(*sqlparser.DDL), false)
	if err != nil {
		return nil, err
	}
	ddl, ok := node.(*plan.CreateTable)
	if !ok {
		return nil, fmt.Errorf("expected translation from type string to sql type has returned an unexpected result")
	}
	// If we successfully created a CreateTable plan with an empty schema then something has gone horribly wrong, so we'll panic
	return ddl.CreateSchema.Schema[0].Type, nil
}

func convert(ctx *sql.Context, stmt sqlparser.Statement, query string) (sql.Node, error) {
	if ss, ok := stmt.(sqlparser.SelectStatement); ok {
		node, err := convertSelectStatement(ctx, ss)
		if err != nil {
			return nil, err
		}
		if into := ss.GetInto(); into != nil {
			node, err = intoToInto(ctx, into, node)
			if err != nil {
				return nil, err
			}
		}
		return node, nil
	}
	switch n := stmt.(type) {
	default:
		return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(n))
	case *sqlparser.Analyze:
		return convertAnalyze(ctx, n, query)
	case *sqlparser.Show:
		// When a query is empty it means it comes from a subquery, as we don't
		// have the query itself in a subquery. Hence, a SHOW could not be
		// parsed.
		if query == "" {
			return nil, sql.ErrUnsupportedFeature.New("SHOW in subquery")
		}
		return convertShow(ctx, n, query)
	case *sqlparser.DDL:
		return convertDDL(ctx, query, n, false)
	case *sqlparser.MultiAlterDDL:
		return convertMultiAlterDDL(ctx, query, n)
	case *sqlparser.DBDDL:
		return convertDBDDL(ctx, n)
	case *sqlparser.Explain:
		return convertExplain(ctx, n)
	case *sqlparser.Insert:
		return convertInsert(ctx, n)
	case *sqlparser.Delete:
		return convertDelete(ctx, n)
	case *sqlparser.Update:
		return convertUpdate(ctx, n)
	case *sqlparser.Load:
		return convertLoad(ctx, n)
	case *sqlparser.Set:
		return convertSet(ctx, n)
	case *sqlparser.Use:
		return convertUse(n)
	case *sqlparser.Begin:
		transChar := sql.ReadWrite
		if n.TransactionCharacteristic == sqlparser.TxReadOnly {
			transChar = sql.ReadOnly
		}

		return plan.NewStartTransaction(transChar), nil
	case *sqlparser.Commit:
		return plan.NewCommit(), nil
	case *sqlparser.Rollback:
		return plan.NewRollback(), nil
	case *sqlparser.Savepoint:
		return plan.NewCreateSavepoint(n.Identifier), nil
	case *sqlparser.RollbackSavepoint:
		return plan.NewRollbackSavepoint(n.Identifier), nil
	case *sqlparser.ReleaseSavepoint:
		return plan.NewReleaseSavepoint(n.Identifier), nil
	case *sqlparser.ChangeReplicationSource:
		return convertChangeReplicationSource(n)
	case *sqlparser.ChangeReplicationFilter:
		return convertChangeReplicationFilter(n)
	case *sqlparser.StartReplica:
		return plan.NewStartReplica(), nil
	case *sqlparser.StopReplica:
		return plan.NewStopReplica(), nil
	case *sqlparser.ResetReplica:
		return plan.NewResetReplica(n.All), nil
	case *sqlparser.BeginEndBlock:
		return convertBeginEndBlock(ctx, n, query)
	case *sqlparser.IfStatement:
		return convertIfBlock(ctx, n)
	case *sqlparser.CaseStatement:
		return convertCaseStatement(ctx, n)
	case *sqlparser.Call:
		return convertCall(ctx, n)
	case *sqlparser.Declare:
		return convertDeclare(ctx, n, query)
	case *sqlparser.FetchCursor:
		return convertFetch(ctx, n)
	case *sqlparser.OpenCursor:
		return convertOpen(ctx, n)
	case *sqlparser.CloseCursor:
		return convertClose(ctx, n)
	case *sqlparser.Loop:
		return convertLoop(ctx, n, query)
	case *sqlparser.Repeat:
		return convertRepeat(ctx, n, query)
	case *sqlparser.While:
		return convertWhile(ctx, n, query)
	case *sqlparser.Leave:
		return convertLeave(ctx, n)
	case *sqlparser.Iterate:
		return convertIterate(ctx, n)
	case *sqlparser.Kill:
		return convertKill(ctx, n)
	case *sqlparser.Signal:
		return convertSignal(ctx, n)
	case *sqlparser.LockTables:
		return convertLockTables(ctx, n)
	case *sqlparser.UnlockTables:
		return convertUnlockTables(ctx, n)
	case *sqlparser.CreateUser:
		return convertCreateUser(ctx, n)
	case *sqlparser.RenameUser:
		return convertRenameUser(ctx, n)
	case *sqlparser.DropUser:
		return plan.NewDropUser(n.IfExists, convertAccountName(n.AccountNames...)), nil
	case *sqlparser.CreateRole:
		return plan.NewCreateRole(n.IfNotExists, convertAccountName(n.Roles...)), nil
	case *sqlparser.DropRole:
		return plan.NewDropRole(n.IfExists, convertAccountName(n.Roles...)), nil
	case *sqlparser.GrantPrivilege:
		return convertGrantPrivilege(ctx, n)
	case *sqlparser.GrantRole:
		return plan.NewGrantRole(
			convertAccountName(n.Roles...),
			convertAccountName(n.To...),
			n.WithAdminOption,
		), nil
	case *sqlparser.GrantProxy:
		return plan.NewGrantProxy(
			convertAccountName(n.On)[0],
			convertAccountName(n.To...),
			n.WithGrantOption,
		), nil
	case *sqlparser.RevokePrivilege:
		return plan.NewRevoke(
			convertPrivilege(n.Privileges...),
			convertObjectType(n.ObjectType),
			convertPrivilegeLevel(n.PrivilegeLevel),
			convertAccountName(n.From...),
			ctx.Session.Client().User,
		)
	case *sqlparser.RevokeAllPrivileges:
		return plan.NewRevokeAll(convertAccountName(n.From...)), nil
	case *sqlparser.RevokeRole:
		return plan.NewRevokeRole(convertAccountName(n.Roles...), convertAccountName(n.From...)), nil
	case *sqlparser.RevokeProxy:
		return plan.NewRevokeProxy(convertAccountName(n.On)[0], convertAccountName(n.From...)), nil
	case *sqlparser.ShowGrants:
		return convertShowGrants(ctx, n)
	case *sqlparser.ShowPrivileges:
		return plan.NewShowPrivileges(), nil
	case *sqlparser.Flush:
		return convertFlush(ctx, n)
	case *sqlparser.Prepare:
		return convertPrepare(ctx, n)
	case *sqlparser.Execute:
		return convertExecute(ctx, n)
	case *sqlparser.Deallocate:
		return convertDeallocate(ctx, n)
	case *sqlparser.CreateSpatialRefSys:
		return convertCreateSpatialRefSys(ctx, n)
	}
}

func convertAnalyze(ctx *sql.Context, n *sqlparser.Analyze, query string) (sql.Node, error) {
	names := make([]sql.DbTable, len(n.Tables))
	for i, table := range n.Tables {
		names[i] = sql.DbTable{Db: table.Qualifier.String(), Table: table.Name.String()}
	}
	return plan.NewAnalyze(names), nil
}

func convertKill(ctx *sql.Context, kill *sqlparser.Kill) (*plan.Kill, error) {
	connID64, err := getInt64Value(ctx, kill.ConnID, "Error parsing KILL, expected int literal")
	if err != nil {
		return nil, err
	}
	connID32 := uint32(connID64)
	if int64(connID32) != connID64 {
		return nil, sql.ErrUnsupportedFeature.New("int literal is not unsigned 32-bit.")
	}
	if kill.Connection {
		return plan.NewKill(plan.KillType_Connection, connID32), nil
	}
	return plan.NewKill(plan.KillType_Query, connID32), nil
}

func convertBlock(ctx *sql.Context, parserStatements sqlparser.Statements, query string) (*plan.Block, error) {
	var statements []sql.Node
	for _, s := range parserStatements {
		statement, err := convert(ctx, s, sqlparser.String(s))
		if err != nil {
			return nil, err
		}
		statements = append(statements, statement)
	}
	return plan.NewBlock(statements), nil
}

func convertBeginEndBlock(ctx *sql.Context, n *sqlparser.BeginEndBlock, query string) (sql.Node, error) {
	block, err := convertBlock(ctx, n.Statements, query)
	if err != nil {
		return nil, err
	}
	return plan.NewBeginEndBlock(n.Label, block), nil
}

func convertIfBlock(ctx *sql.Context, n *sqlparser.IfStatement) (sql.Node, error) {
	ifConditionals := make([]*plan.IfConditional, len(n.Conditions))
	for i, ic := range n.Conditions {
		ifConditional, err := convertIfConditional(ctx, ic)
		if err != nil {
			return nil, err
		}
		ifConditionals[i] = ifConditional
	}
	elseBlock, err := convertBlock(ctx, n.Else, "compound statement in else block")
	if err != nil {
		return nil, err
	}
	return plan.NewIfElse(ifConditionals, elseBlock), nil
}

func convertCaseStatement(ctx *sql.Context, n *sqlparser.CaseStatement) (sql.Node, error) {
	ifConditionals := make([]*plan.IfConditional, len(n.Cases))
	for i, c := range n.Cases {
		ifConditional, err := convertIfConditional(ctx, sqlparser.IfStatementCondition{
			Expr:       c.Case,
			Statements: c.Statements,
		})
		if err != nil {
			return nil, err
		}
		ifConditionals[i] = ifConditional
	}
	var elseBlock sql.Node
	if n.Else != nil {
		var err error
		elseBlock, err = convertBlock(ctx, n.Else, "compound statement in else block")
		if err != nil {
			return nil, err
		}
	}
	if n.Expr == nil {
		return plan.NewCaseStatement(nil, ifConditionals, elseBlock), nil
	} else {
		caseExpr, err := ExprToExpression(ctx, n.Expr)
		if err != nil {
			return nil, err
		}
		return plan.NewCaseStatement(caseExpr, ifConditionals, elseBlock), nil
	}
}

func convertIfConditional(ctx *sql.Context, n sqlparser.IfStatementCondition) (*plan.IfConditional, error) {
	block, err := convertBlock(ctx, n.Statements, "compound statement in if block")
	if err != nil {
		return nil, err
	}
	condition, err := ExprToExpression(ctx, n.Expr)
	if err != nil {
		return nil, err
	}
	return plan.NewIfConditional(condition, block), nil
}

func convertSelectStatement(ctx *sql.Context, ss sqlparser.SelectStatement) (sql.Node, error) {
	switch n := ss.(type) {
	case *sqlparser.Select:
		return convertSelect(ctx, n)
	case *sqlparser.Union:
		return convertUnion(ctx, n)
	case *sqlparser.ParenSelect:
		return convertSelectStatement(ctx, n.Select)
	default:
		return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(n))
	}
}

func convertExplain(ctx *sql.Context, n *sqlparser.Explain) (sql.Node, error) {
	child, err := convert(ctx, n.Statement, "")
	if err != nil {
		return nil, err
	}

	explainFmt := sqlparser.TreeStr
	switch strings.ToLower(n.ExplainFormat) {
	case "", sqlparser.TreeStr:
	// tree format, do nothing
	case "debug":
		explainFmt = "debug"
	default:
		return nil, errInvalidDescribeFormat.New(
			n.ExplainFormat,
			strings.Join(describeSupportedFormats, ", "),
		)
	}

	return plan.NewDescribeQuery(explainFmt, child), nil
}

func convertPrepare(ctx *sql.Context, n *sqlparser.Prepare) (sql.Node, error) {
	expr := n.Expr
	if strings.HasPrefix(n.Expr, "@") {
		varName := strings.ToLower(strings.Trim(n.Expr, "@"))
		_, val, err := ctx.GetUserVariable(ctx, varName)
		if err != nil {
			return nil, err
		}
		strVal, _, err := types.LongText.Convert(val)
		if err != nil {
			return nil, err
		}
		if strVal == nil {
			expr = "NULL"
		} else {
			expr = strVal.(string)
		}
	}

	childStmt, err := sqlparser.Parse(expr)
	if err != nil {
		return nil, err
	}

	child, err := convert(ctx, childStmt, expr)
	if err != nil {
		return nil, err
	}

	return plan.NewPrepareQuery(n.Name, child), nil
}

func convertExecute(ctx *sql.Context, n *sqlparser.Execute) (sql.Node, error) {
	exprs := make([]sql.Expression, len(n.VarList))
	for i, e := range n.VarList {
		if strings.HasPrefix(e, "@") {
			exprs[i] = expression.NewUserVar(strings.TrimPrefix(e, "@"))
		} else {
			exprs[i] = expression.NewUnresolvedProcedureParam(e)
		}
	}
	return plan.NewExecuteQuery(n.Name, exprs...), nil
}

func convertDeallocate(ctx *sql.Context, n *sqlparser.Deallocate) (sql.Node, error) {
	return plan.NewDeallocateQuery(n.Name), nil
}

func convertUse(n *sqlparser.Use) (sql.Node, error) {
	name := n.DBName.String()
	return plan.NewUse(sql.UnresolvedDatabase(name)), nil
}

func convertSet(ctx *sql.Context, n *sqlparser.Set) (sql.Node, error) {
	var setVarExprs []*sqlparser.SetVarExpr
	for _, setExpr := range n.Exprs {
		switch strings.ToLower(setExpr.Name.String()) {
		case "names":
			// Special case: SET NAMES expands to 3 different system variables.
			setVarExprs = append(setVarExprs, getSetVarExprsFromSetNamesExpr(setExpr)...)
		case "charset":
			// Special case: SET CHARACTER SET (CHARSET) expands to 3 different system variables.
			csd, err := ctx.GetSessionVariable(ctx, "character_set_database")
			if err != nil {
				return nil, err
			}
			setVarExprs = append(setVarExprs, getSetVarExprsFromSetCharsetExpr(setExpr, []byte(csd.(string)))...)
		default:
			setVarExprs = append(setVarExprs, setExpr)
		}
	}

	exprs, err := setExprsToExpressions(ctx, setVarExprs)
	if err != nil {
		return nil, err
	}

	return plan.NewSet(exprs), nil
}

func getSetVarExprsFromSetNamesExpr(expr *sqlparser.SetVarExpr) []*sqlparser.SetVarExpr {
	return []*sqlparser.SetVarExpr{
		{
			Name: sqlparser.NewColName("character_set_client"),
			Expr: expr.Expr,
		},
		{
			Name: sqlparser.NewColName("character_set_connection"),
			Expr: expr.Expr,
		},
		{
			Name: sqlparser.NewColName("character_set_results"),
			Expr: expr.Expr,
		},
		// TODO (9/24/20 Zach): this should also set the collation_connection to the default collation for the character set named
	}
}

func getSetVarExprsFromSetCharsetExpr(expr *sqlparser.SetVarExpr, csd []byte) []*sqlparser.SetVarExpr {
	return []*sqlparser.SetVarExpr{
		{
			Name: sqlparser.NewColName("character_set_client"),
			Expr: expr.Expr,
		},
		{
			Name: sqlparser.NewColName("character_set_results"),
			Expr: expr.Expr,
		},
		{
			Name: sqlparser.NewColName("character_set_connection"),
			Expr: &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: csd},
		},
	}
}

func convertChangeReplicationSource(n *sqlparser.ChangeReplicationSource) (sql.Node, error) {
	convertedOptions := make([]binlogreplication.ReplicationOption, 0, len(n.Options))
	for _, option := range n.Options {
		convertedOption, err := convertReplicationOption(option)
		if err != nil {
			return nil, err
		}
		convertedOptions = append(convertedOptions, *convertedOption)
	}
	return plan.NewChangeReplicationSource(convertedOptions), nil
}

func convertReplicationOption(option *sqlparser.ReplicationOption) (*binlogreplication.ReplicationOption, error) {
	if option.Value == nil {
		return nil, fmt.Errorf("nil replication option specified for option %q", option.Name)
	}
	switch vv := option.Value.(type) {
	case string:
		return binlogreplication.NewReplicationOption(option.Name, binlogreplication.StringReplicationOptionValue{Value: vv}), nil
	case int:
		return binlogreplication.NewReplicationOption(option.Name, binlogreplication.IntegerReplicationOptionValue{Value: vv}), nil
	case sqlparser.TableNames:
		urts := make([]sql.UnresolvedTable, len(vv))
		for i, tableName := range vv {
			urts[i] = tableNameToUnresolvedTable(tableName)
		}
		return binlogreplication.NewReplicationOption(option.Name, binlogreplication.TableNamesReplicationOptionValue{Value: urts}), nil
	default:
		return nil, fmt.Errorf("unsupported option value type '%T' specified for option %q", option.Value, option.Name)
	}
}

func convertChangeReplicationFilter(n *sqlparser.ChangeReplicationFilter) (sql.Node, error) {
	convertedOptions := make([]binlogreplication.ReplicationOption, 0, len(n.Options))
	for _, option := range n.Options {
		convertedOption, err := convertReplicationOption(option)
		if err != nil {
			return nil, err
		}
		convertedOptions = append(convertedOptions, *convertedOption)
	}
	return plan.NewChangeReplicationFilter(convertedOptions), nil
}

func convertShow(ctx *sql.Context, s *sqlparser.Show, query string) (sql.Node, error) {
	showType := strings.ToLower(s.Type)
	switch showType {
	case "processlist":
		return plan.NewShowProcessList(), nil
	case sqlparser.CreateTableStr, "create view":
		var asOfExpression sql.Expression
		if s.ShowTablesOpt != nil && s.ShowTablesOpt.AsOf != nil {
			expr, err := ExprToExpression(ctx, s.ShowTablesOpt.AsOf)
			if err != nil {
				return nil, err
			}
			asOfExpression = expr
		}
		table := tableNameToUnresolvedTableAsOf(s.Table, asOfExpression)
		return plan.NewShowCreateTableWithAsOf(table, showType == "create view", asOfExpression), nil
	case "create database", "create schema":
		return plan.NewShowCreateDatabase(
			sql.UnresolvedDatabase(s.Database),
			s.IfNotExists,
		), nil
	case sqlparser.CreateTriggerStr:
		udb, err := getUnresolvedDatabase(ctx, s.Table.Qualifier.String())
		if err != nil {
			return nil, err
		}
		return plan.NewShowCreateTrigger(udb, s.Table.Name.String()), nil
	case sqlparser.CreateProcedureStr:
		udb, err := getUnresolvedDatabase(ctx, s.Table.Qualifier.String())
		if err != nil {
			return nil, err
		}
		return plan.NewShowCreateProcedure(udb, s.Table.Name.String()), nil
	case sqlparser.CreateEventStr:
		udb, err := getUnresolvedDatabase(ctx, s.Table.Qualifier.String())
		if err != nil {
			return nil, err
		}
		return plan.NewShowCreateEvent(udb, s.Table.Name.String()), nil
	case "triggers":
		var dbName string
		var filter sql.Expression

		if s.ShowTablesOpt != nil {
			dbName = s.ShowTablesOpt.DbName
			if s.ShowTablesOpt.Filter != nil {
				if s.ShowTablesOpt.Filter.Filter != nil {
					var err error
					filter, err = ExprToExpression(ctx, s.ShowTablesOpt.Filter.Filter)
					if err != nil {
						return nil, err
					}
				} else if s.ShowTablesOpt.Filter.Like != "" {
					filter = expression.NewLike(
						expression.NewUnresolvedColumn("Table"),
						expression.NewLiteral(s.ShowTablesOpt.Filter.Like, types.LongText),
						nil,
					)
				}
			}
		}

		var node sql.Node = plan.NewShowTriggers(sql.UnresolvedDatabase(dbName))
		if filter != nil {
			node = plan.NewFilter(filter, node)
		}

		return node, nil
	case "events":
		var dbName string
		var filter sql.Expression
		if s.ShowTablesOpt != nil {
			dbName = s.ShowTablesOpt.DbName
			if s.ShowTablesOpt.Filter != nil {
				if s.ShowTablesOpt.Filter.Filter != nil {
					var err error
					filter, err = ExprToExpression(ctx, s.ShowTablesOpt.Filter.Filter)
					if err != nil {
						return nil, err
					}
				} else if s.ShowTablesOpt.Filter.Like != "" {
					filter = expression.NewLike(
						expression.NewUnresolvedColumn("Name"),
						expression.NewLiteral(s.ShowTablesOpt.Filter.Like, types.LongText),
						nil,
					)
				}
			}
		}

		var node sql.Node = plan.NewShowEvents(sql.UnresolvedDatabase(dbName))
		if filter != nil {
			node = plan.NewFilter(filter, node)
		}

		return node, nil
	case sqlparser.ProcedureStatusStr:
		var filter sql.Expression

		node, err := Parse(ctx, "select routine_schema as `Db`, routine_name as `Name`, routine_type as `Type`,"+
			"definer as `Definer`, last_altered as `Modified`, created as `Created`, security_type as `Security_type`,"+
			"routine_comment as `Comment`, CHARACTER_SET_CLIENT as `character_set_client`, COLLATION_CONNECTION as `collation_connection`,"+
			"database_collation as `Database Collation` from information_schema.routines where routine_type = 'PROCEDURE'")
		if err != nil {
			return nil, err
		}

		if s.Filter != nil {
			if s.Filter.Filter != nil {
				var err error
				filter, err = ExprToExpression(ctx, s.Filter.Filter)
				if err != nil {
					return nil, err
				}
			} else if s.Filter.Like != "" {
				filter = expression.NewLike(
					expression.NewUnresolvedColumn("Name"),
					expression.NewLiteral(s.Filter.Like, types.LongText),
					nil,
				)
			}
		}

		if filter != nil {
			node = plan.NewHaving(filter, node)
		}
		return node, nil
	case sqlparser.FunctionStatusStr:
		var filter sql.Expression
		var node sql.Node
		if s.Filter != nil {
			if s.Filter.Filter != nil {
				var err error
				filter, err = ExprToExpression(ctx, s.Filter.Filter)
				if err != nil {
					return nil, err
				}
			} else if s.Filter.Like != "" {
				filter = expression.NewLike(
					expression.NewUnresolvedColumn("Name"),
					expression.NewLiteral(s.Filter.Like, types.LongText),
					nil,
				)
			}
		}

		node, err := Parse(ctx, "select routine_schema as `Db`, routine_name as `Name`, routine_type as `Type`,"+
			"definer as `Definer`, last_altered as `Modified`, created as `Created`, security_type as `Security_type`,"+
			"routine_comment as `Comment`, character_set_client, collation_connection,"+
			"database_collation as `Database Collation` from information_schema.routines where routine_type = 'FUNCTION'")
		if err != nil {
			return nil, err
		}

		if filter != nil {
			node = plan.NewHaving(filter, node)
		}
		return node, nil
	case sqlparser.TableStatusStr:
		return convertShowTableStatus(ctx, s)
	case "index":
		return plan.NewShowIndexes(plan.NewUnresolvedTable(s.Table.Name.String(), s.Table.Qualifier.String())), nil
	case sqlparser.KeywordString(sqlparser.VARIABLES):
		var filter sql.Expression
		var like sql.Expression
		var err error
		if s.Filter != nil {
			if s.Filter.Filter != nil {
				filter, err = ExprToExpression(ctx, s.Filter.Filter)
				if err != nil {
					return nil, err
				}
				filter, _, err = transform.Expr(filter, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
					switch e.(type) {
					case *expression.UnresolvedColumn:
						if strings.ToLower(e.String()) != "variable_name" {
							return nil, transform.SameTree, sql.ErrUnsupportedFeature.New("WHERE clause supports only 'variable_name' column for SHOW VARIABLES")
						}
						return expression.NewGetField(0, types.Text, "variable_name", true), transform.NewTree, nil
					default:
						return e, transform.SameTree, nil
					}
				})
				if err != nil {
					return nil, err
				}
			}
			if s.Filter.Like != "" {
				like = expression.NewLike(
					expression.NewGetField(0, types.LongText, "variable_name", false),
					expression.NewLiteral(s.Filter.Like, types.LongText),
					nil,
				)
				if filter != nil {
					filter = expression.NewAnd(like, filter)
				} else {
					filter = like
				}
			}
		}

		return plan.NewShowVariables(filter, strings.ToLower(s.Scope) == "global"), nil
	case sqlparser.KeywordString(sqlparser.TABLES):
		var dbName string
		var filter sql.Expression
		var asOf sql.Expression
		var full bool

		if s.ShowTablesOpt != nil {
			dbName = s.ShowTablesOpt.DbName
			if dbName == "" {
				dbName = ctx.GetCurrentDatabase()
			}
			full = s.Full

			if s.ShowTablesOpt.Filter != nil {
				if s.ShowTablesOpt.Filter.Filter != nil {
					var err error
					filter, err = ExprToExpression(ctx, s.ShowTablesOpt.Filter.Filter)
					if err != nil {
						return nil, err
					}
				} else if s.ShowTablesOpt.Filter.Like != "" {
					filter = expression.NewLike(
						expression.NewUnresolvedColumn(fmt.Sprintf("Tables_in_%s", dbName)),
						expression.NewLiteral(s.ShowTablesOpt.Filter.Like, types.LongText),
						nil,
					)
				}
			}

			if s.ShowTablesOpt.AsOf != nil {
				var err error
				asOf, err = ExprToExpression(ctx, s.ShowTablesOpt.AsOf)
				if err != nil {
					return nil, err
				}
			}
		}

		var node sql.Node = plan.NewShowTables(sql.UnresolvedDatabase(dbName), full, asOf)
		if filter != nil {
			node = plan.NewFilter(filter, node)
		}

		return node, nil
	case sqlparser.KeywordString(sqlparser.DATABASES), sqlparser.KeywordString(sqlparser.SCHEMAS):
		var node sql.Node = plan.NewShowDatabases()
		var filter sql.Expression
		if s.Filter != nil {
			if s.Filter.Filter != nil {
				var err error
				filter, err = ExprToExpression(ctx, s.Filter.Filter)
				if err != nil {
					return nil, err
				}
			} else if s.Filter.Like != "" {
				filter = expression.NewLike(
					expression.NewUnresolvedColumn("Database"),
					expression.NewLiteral(s.Filter.Like, types.LongText),
					nil,
				)
			}
		}
		if filter != nil {
			node = plan.NewFilter(filter, node)
		}
		return node, nil
	case sqlparser.KeywordString(sqlparser.FIELDS), sqlparser.KeywordString(sqlparser.COLUMNS):
		var asOfExpression sql.Expression
		if s.ShowTablesOpt != nil && s.ShowTablesOpt.AsOf != nil {
			expression, err := ExprToExpression(ctx, s.ShowTablesOpt.AsOf)
			if err != nil {
				return nil, err
			}
			asOfExpression = expression
		}

		table := tableNameToUnresolvedTableAsOf(s.Table, asOfExpression)
		full := s.Full

		var node sql.Node = plan.NewShowColumns(full, table)

		if s.ShowTablesOpt != nil && s.ShowTablesOpt.Filter != nil {
			if s.ShowTablesOpt.Filter.Like != "" {
				pattern := expression.NewLiteral(s.ShowTablesOpt.Filter.Like, types.LongText)

				node = plan.NewFilter(
					expression.NewLike(
						expression.NewUnresolvedColumn("Field"),
						pattern,
						nil,
					),
					node,
				)
			}

			if s.ShowTablesOpt.Filter.Filter != nil {
				filter, err := ExprToExpression(ctx, s.ShowTablesOpt.Filter.Filter)
				if err != nil {
					return nil, err
				}

				node = plan.NewFilter(filter, node)
			}
		}

		return node, nil
	case sqlparser.KeywordString(sqlparser.WARNINGS):
		if s.CountStar {
			unsupportedShow := "SHOW COUNT(*) WARNINGS"
			return nil, sql.ErrUnsupportedFeature.New(unsupportedShow)
		}
		var node sql.Node
		var err error
		node = plan.ShowWarnings(ctx.Session.Warnings())
		if s.Limit != nil {
			if s.Limit.Offset != nil {
				node, err = offsetToOffset(ctx, s.Limit.Offset, node)
				if err != nil {
					return nil, err
				}
			}

			node, err = limitToLimit(ctx, s.Limit.Rowcount, node)
			if err != nil {
				return nil, err
			}
		}
		return node, nil
	case sqlparser.KeywordString(sqlparser.COLLATION):
		// show collation statements are functionally identical to selecting from the collations table in
		// information_schema, with slightly different syntax and with some columns aliased.
		// TODO: install information_schema automatically for all catalogs
		infoSchemaSelect, err := Parse(ctx, "select collation_name as `collation`, character_set_name as charset, id,"+
			"is_default as `default`, is_compiled as compiled, sortlen, pad_attribute from information_schema.collations")
		if err != nil {
			return nil, err
		}

		if s.ShowCollationFilterOpt != nil {
			filterExpr, err := ExprToExpression(ctx, s.ShowCollationFilterOpt)
			if err != nil {
				return nil, err
			}
			// TODO: once collations are properly implemented, we should better be able to handle utf8 -> utf8mb3 comparisons as they're aliases
			filterExpr, _, err = transform.Expr(filterExpr, func(expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				if exprLiteral, ok := expr.(*expression.Literal); ok {
					const utf8Prefix = "utf8_"
					if strLiteral, ok := exprLiteral.Value().(string); ok && strings.HasPrefix(strLiteral, utf8Prefix) {
						return expression.NewLiteral("utf8mb3_"+strLiteral[len(utf8Prefix):], exprLiteral.Type()), transform.NewTree, nil
					}
				}
				return expr, transform.SameTree, nil
			})
			if err != nil {
				return nil, err
			}
			return plan.NewHaving(filterExpr, infoSchemaSelect), nil
		}

		return infoSchemaSelect, nil
	case sqlparser.KeywordString(sqlparser.CHARSET):
		var filter sql.Expression

		if s.Filter != nil {
			if s.Filter.Filter != nil {
				var err error
				filter, err = ExprToExpression(ctx, s.Filter.Filter)
				if err != nil {
					return nil, err
				}
			} else if s.Filter.Like != "" {
				filter = expression.NewLike(
					expression.NewUnresolvedColumn("Charset"),
					expression.NewLiteral(s.Filter.Like, types.LongText),
					nil,
				)
			}
		}

		var node sql.Node = plan.NewShowCharset()
		if filter != nil {
			node = plan.NewFilter(filter, node)
		}
		return node, nil
	case sqlparser.KeywordString(sqlparser.ENGINES):
		infoSchemaSelect, err := Parse(ctx, "select * from information_schema.engines")
		if err != nil {
			return nil, err
		}

		return infoSchemaSelect, nil
	case sqlparser.KeywordString(sqlparser.STATUS):
		var node sql.Node
		if s.Scope == sqlparser.GlobalStr {
			node = plan.NewShowStatus(plan.ShowStatusModifier_Global)
		} else {
			node = plan.NewShowStatus(plan.ShowStatusModifier_Session)
		}

		var filter sql.Expression
		if s.Filter != nil {
			if s.Filter.Like != "" {
				filter = expression.NewLike(
					expression.NewUnresolvedColumn("Variable_name"),
					expression.NewLiteral(s.Filter.Like, types.LongText),
					nil,
				)
			} else if s.Filter.Filter != nil {
				var err error
				filter, err = ExprToExpression(ctx, s.Filter.Filter)
				if err != nil {
					return nil, err
				}
			}
		}

		if filter != nil {
			node = plan.NewFilter(filter, node)
		}

		return node, nil
	case "replica status":
		return plan.NewShowReplicaStatus(), nil

	default:
		unsupportedShow := fmt.Sprintf("SHOW %s", s.Type)
		return nil, sql.ErrUnsupportedFeature.New(unsupportedShow)
	}
}

func convertUnion(ctx *sql.Context, u *sqlparser.Union) (sql.Node, error) {
	left, err := convertSelectStatement(ctx, u.Left)
	if err != nil {
		return nil, err
	}
	right, err := convertSelectStatement(ctx, u.Right)
	if err != nil {
		return nil, err
	}

	// TODO: CalcFoundRows?
	distinct := u.Type != sqlparser.UnionAllStr
	l, err := limitToLimitExpr(ctx, u.Limit)
	if err != nil {
		return nil, err
	}
	off, err := offsetToOffsetExpr(ctx, u.Limit)
	if err != nil {
		return nil, err
	}

	var sortFields sql.SortFields
	if len(u.OrderBy) > 0 {
		sortFields, err = orderByToSortFields(ctx, u.OrderBy)
		if err != nil {
			return nil, err
		}
	}
	union, err := buildUnion(left, right, distinct, l, off, sortFields)
	if err != nil {
		return nil, err
	}
	if u.With != nil {
		return ctesToWith(ctx, u.With, union)
	}
	return union, nil
}

func buildUnion(left, right sql.Node, distinct bool, limit, offset sql.Expression, sf sql.SortFields) (*plan.Union, error) {
	// propagate sortFields, limit from child
	n, ok := left.(*plan.Union)
	if ok {
		if len(n.SortFields) > 0 {
			if len(sf) > 0 {
				return nil, sql.ErrConflictingExternalQuery.New()
			}
			sf = n.SortFields
		}
		if n.Limit != nil {
			if limit != nil {
				return nil, fmt.Errorf("conflicing external LIMIT")
			}
			limit = n.Limit
		}
		if n.Offset != nil {
			if offset != nil {
				return nil, fmt.Errorf("conflicing external OFFSET")
			}
			offset = n.Offset
		}
		left = plan.NewUnion(n.Left(), n.Right(), n.Distinct, nil, nil, nil)
		// TODO recurse and put more union-specific rules after
	}
	return plan.NewUnion(left, right, distinct, limit, offset, sf), nil
}

func convertSelect(ctx *sql.Context, s *sqlparser.Select) (sql.Node, error) {
	node, err := tableExprsToTable(ctx, s.From)
	if err != nil {
		return nil, err
	}

	// If the top level node can store comments and one was provided, store it.
	if cn, ok := node.(sql.CommentedNode); ok && len(s.Comments) > 0 {
		node = cn.WithComment(string(s.Comments[0]))
	}

	if s.Where != nil {
		node, err = whereToFilter(ctx, s.Where, node)
		if err != nil {
			return nil, err
		}
	}

	node, err = selectToSelectionNode(ctx, s.SelectExprs, s.GroupBy, node)
	if err != nil {
		return nil, err
	}

	if window, ok := node.(*plan.Window); ok && s.Window != nil {
		node, err = windowToWindow(ctx, s.Window, window)
		if err != nil {
			return nil, err
		}
	}

	if s.Having != nil {
		node, err = havingToHaving(ctx, s.Having, node)
		if err != nil {
			return nil, err
		}
	}

	if s.Distinct != "" {
		node = plan.NewDistinct(node)
	}

	node, err = nodeWithLimitAndOrderBy(ctx, node, s.OrderBy, s.Limit, s.CalcFoundRows)
	if err != nil {
		return nil, err
	}

	// Build With node if provided
	if s.With != nil {
		node, err = ctesToWith(ctx, s.With, node)
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

func limitToLimitExpr(ctx *sql.Context, limit *sqlparser.Limit) (sql.Expression, error) {
	if limit != nil {
		return ExprToExpression(ctx, limit.Rowcount)
	}
	return nil, nil
}

func offsetToOffsetExpr(ctx *sql.Context, limit *sqlparser.Limit) (sql.Expression, error) {
	if limit != nil && limit.Offset != nil {
		return ExprToExpression(ctx, limit.Offset)
	}
	return nil, nil
}

func nodeWithLimitAndOrderBy(ctx *sql.Context, node sql.Node, orderby sqlparser.OrderBy, limit *sqlparser.Limit, calcfoundrows bool) (sql.Node, error) {
	var err error

	if len(orderby) != 0 {
		node, err = orderByToSort(ctx, orderby, node)
		if err != nil {
			return nil, err
		}
	}

	// Limit must wrap offset, and not vice-versa, so that skipped rows don't count toward the returned row count.
	if limit != nil && limit.Offset != nil {
		node, err = offsetToOffset(ctx, limit.Offset, node)
		if err != nil {
			return nil, err
		}
	}

	if limit != nil {
		l, err := limitToLimit(ctx, limit.Rowcount, node)
		if err != nil {
			return nil, err
		}

		if calcfoundrows {
			l.CalcFoundRows = true
		}

		node = l
	}

	return node, nil
}

func ctesToWith(ctx *sql.Context, with *sqlparser.With, node sql.Node) (sql.Node, error) {
	ctes := make([]*plan.CommonTableExpression, len(with.Ctes))
	for i, cteExpr := range with.Ctes {
		var err error
		ctes[i], err = cteExprToCte(ctx, cteExpr)
		if err != nil {
			return nil, err
		}
	}

	return plan.NewWith(node, ctes, with.Recursive), nil
}

func intoToInto(ctx *sql.Context, into *sqlparser.Into, node sql.Node) (sql.Node, error) {
	if into.Outfile != "" || into.Dumpfile != "" {
		return nil, sql.ErrUnsupportedSyntax.New("select into files is not supported yet")
	}

	vars := make([]sql.Expression, len(into.Variables))
	for i, val := range into.Variables {
		if strings.HasPrefix(val.String(), "@") {
			vars[i] = expression.NewUserVar(strings.TrimPrefix(val.String(), "@"))
		} else {
			vars[i] = expression.NewUnresolvedProcedureParam(val.String())
		}
	}
	return plan.NewInto(node, vars), nil
}

func cteExprToCte(ctx *sql.Context, expr sqlparser.TableExpr) (*plan.CommonTableExpression, error) {
	cte, ok := expr.(*sqlparser.CommonTableExpr)
	if !ok {
		return nil, sql.ErrUnsupportedFeature.New(fmt.Sprintf("Unsupported type of common table expression %T", expr))
	}

	ate := cte.AliasedTableExpr
	_, ok = ate.Expr.(*sqlparser.Subquery)
	if !ok {
		return nil, sql.ErrUnsupportedFeature.New(fmt.Sprintf("Unsupported type of common table expression %T", ate.Expr))
	}

	subquery, err := tableExprToTable(ctx, ate)
	if err != nil {
		return nil, err
	}

	columns := columnsToStrings(cte.Columns)

	return plan.NewCommonTableExpression(subquery.(*plan.SubqueryAlias), columns), nil
}

func convertDDL(ctx *sql.Context, query string, c *sqlparser.DDL, multiAlterDDL bool) (sql.Node, error) {
	switch strings.ToLower(c.Action) {
	case sqlparser.CreateStr:
		if c.TriggerSpec != nil {
			return convertCreateTrigger(ctx, query, c)
		}
		if c.ProcedureSpec != nil {
			return convertCreateProcedure(ctx, query, c)
		}
		if c.EventSpec != nil {
			return convertCreateEvent(ctx, query, c)
		}
		if c.ViewSpec != nil {
			return convertCreateView(ctx, query, c)
		}
		return convertCreateTable(ctx, c)
	case sqlparser.DropStr:
		if c.TriggerSpec != nil {
			return plan.NewDropTrigger(sql.UnresolvedDatabase(c.TriggerSpec.TrigName.Qualifier.String()), c.TriggerSpec.TrigName.Name.String(), c.IfExists), nil
		}
		if c.ProcedureSpec != nil {
			return plan.NewDropProcedure(sql.UnresolvedDatabase(c.ProcedureSpec.ProcName.Qualifier.String()),
				c.ProcedureSpec.ProcName.Name.String(), c.IfExists), nil
		}
		if c.EventSpec != nil {
			return plan.NewDropEvent(sql.UnresolvedDatabase(c.EventSpec.EventName.Qualifier.String()),
				c.EventSpec.EventName.Name.String(), c.IfExists), nil
		}
		if len(c.FromViews) != 0 {
			return convertDropView(ctx, c)
		}
		return convertDropTable(ctx, c)
	case sqlparser.AlterStr:
		if c.EventSpec != nil {
			return convertAlterEvent(ctx, query, c)
		}
		return convertAlterTable(ctx, c)
	case sqlparser.RenameStr:
		return convertRenameTable(ctx, c, multiAlterDDL)
	case sqlparser.TruncateStr:
		return convertTruncateTable(ctx, c)
	default:
		return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(c))
	}
}

// convertMultiAlterDDL converts MultiAlterDDL statements
// If there are multiple alter statements, they are sorted in order of their precedence and placed inside a plan.Block
// Currently, the precedence of DDL statements is:
// 1.  RENAME COLUMN
// 2.  DROP COLUMN
// 3.  MODIFY COLUMN
// 4.  ADD COLUMN
// 5.  DROP CHECK/CONSTRAINT
// 7.  CREATE CHECK/CONSTRAINT
// 8.  RENAME INDEX
// 9.  DROP INDEX
// 10. ADD INDEX
func convertMultiAlterDDL(ctx *sql.Context, query string, c *sqlparser.MultiAlterDDL) (sql.Node, error) {
	statementsLen := len(c.Statements)
	if statementsLen == 1 {
		return convertDDL(ctx, query, c.Statements[0], true)
	}
	statements := make([]sql.Node, statementsLen)
	var err error
	for i := 0; i < statementsLen; i++ {
		statements[i], err = convertDDL(ctx, query, c.Statements[i], true)
		if err != nil {
			return nil, err
		}
	}

	// TODO: add correct precedence for ADD/DROP PRIMARY KEY and (maybe) FOREIGN KEY
	// certain alter statements need to happen before others
	sort.Slice(statements, func(i, j int) bool {
		switch ii := statements[i].(type) {
		case *plan.RenameColumn:
			switch statements[j].(type) {
			case *plan.DropColumn,
				*plan.ModifyColumn,
				*plan.AddColumn,
				*plan.DropConstraint,
				*plan.DropCheck,
				*plan.CreateCheck,
				*plan.AlterIndex:
				return true
			}
		case *plan.DropColumn:
			switch statements[j].(type) {
			case *plan.ModifyColumn,
				*plan.AddColumn,
				*plan.DropConstraint,
				*plan.DropCheck,
				*plan.CreateCheck,
				*plan.AlterIndex:
				return true
			}
		case *plan.ModifyColumn:
			switch statements[j].(type) {
			case *plan.AddColumn,
				*plan.DropConstraint,
				*plan.DropCheck,
				*plan.CreateCheck,
				*plan.AlterIndex:
				return true
			}
		case *plan.AddColumn:
			switch statements[j].(type) {
			case *plan.DropConstraint,
				*plan.DropCheck,
				*plan.CreateCheck,
				*plan.AlterIndex:
				return true
			}
		case *plan.DropConstraint:
			switch statements[j].(type) {
			case *plan.DropCheck,
				*plan.CreateCheck,
				*plan.AlterIndex:
				return true
			}
		case *plan.DropCheck:
			switch statements[j].(type) {
			case *plan.CreateCheck,
				*plan.AlterIndex:
				return true
			}
		case *plan.CreateCheck:
			switch statements[j].(type) {
			case *plan.AlterIndex:
				return true
			}
		// AlterIndex precedence is Rename, Drop, then Create
		// So statement[i] < statement[j] = statement[i].action > statement[j].action
		case *plan.AlterIndex:
			switch jj := statements[j].(type) {
			case *plan.AlterIndex:
				return ii.Action > jj.Action
			}
		}
		return false
	})
	return plan.NewBlock(statements), nil
}

func convertDBDDL(ctx *sql.Context, c *sqlparser.DBDDL) (sql.Node, error) {
	switch strings.ToLower(c.Action) {
	case sqlparser.CreateStr:
		var charsetStr *string
		var collationStr *string
		for _, cc := range c.CharsetCollate {
			ccType := strings.ToLower(cc.Type)
			if ccType == "character set" {
				val := cc.Value
				charsetStr = &val
			} else if ccType == "collate" {
				val := cc.Value
				collationStr = &val
			} else {
				ctx.Session.Warn(&sql.Warning{
					Level:   "Warning",
					Code:    mysql.ERNotSupportedYet,
					Message: "Setting CHARACTER SET, COLLATION and ENCRYPTION are not supported yet",
				})
			}
		}
		collation, err := sql.ParseCollation(charsetStr, collationStr, false)
		if err != nil {
			return nil, err
		}
		return plan.NewCreateDatabase(c.DBName, c.IfNotExists, collation), nil
	case sqlparser.DropStr:
		return plan.NewDropDatabase(c.DBName, c.IfExists), nil
	case sqlparser.AlterStr:
		if len(c.CharsetCollate) == 0 {
			if len(c.DBName) > 0 {
				return nil, sql.ErrSyntaxError.New(fmt.Sprintf("alter database %s", c.DBName))
			} else {
				return nil, sql.ErrSyntaxError.New("alter database")
			}
		}

		var charsetStr *string
		var collationStr *string
		for _, cc := range c.CharsetCollate {
			ccType := strings.ToLower(cc.Type)
			if ccType == "character set" {
				val := cc.Value
				charsetStr = &val
			} else if ccType == "collate" {
				val := cc.Value
				collationStr = &val
			}
		}
		collation, err := sql.ParseCollation(charsetStr, collationStr, false)
		if err != nil {
			return nil, err
		}
		return plan.NewAlterDatabase(c.DBName, collation), nil
	default:
		return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(c))
	}
}

func convertCreateTrigger(ctx *sql.Context, query string, c *sqlparser.DDL) (sql.Node, error) {
	var triggerOrder *plan.TriggerOrder
	if c.TriggerSpec.Order != nil {
		triggerOrder = &plan.TriggerOrder{
			PrecedesOrFollows: c.TriggerSpec.Order.PrecedesOrFollows,
			OtherTriggerName:  c.TriggerSpec.Order.OtherTriggerName,
		}
	} else {
		//TODO: fix vitess->sql.y, in CREATE TRIGGER, if trigger_order_opt evaluates to empty then SubStatementPositionStart swallows the first token of the body
		beforeSwallowedToken := strings.LastIndexFunc(strings.TrimRightFunc(query[:c.SubStatementPositionStart], unicode.IsSpace), unicode.IsSpace)
		if beforeSwallowedToken != -1 {
			c.SubStatementPositionStart = beforeSwallowedToken
		}
	}

	bodyStr := strings.TrimSpace(query[c.SubStatementPositionStart:c.SubStatementPositionEnd])
	body, err := convert(ctx, c.TriggerSpec.Body, bodyStr)
	if err != nil {
		return nil, err
	}
	definer := getCurrentUserForDefiner(ctx, c.TriggerSpec.Definer)

	return plan.NewCreateTrigger(
		sql.UnresolvedDatabase(c.TriggerSpec.TrigName.Qualifier.String()),
		c.TriggerSpec.TrigName.Name.String(),
		c.TriggerSpec.Time,
		c.TriggerSpec.Event,
		triggerOrder,
		tableNameToUnresolvedTable(c.Table),
		body,
		query,
		bodyStr,
		ctx.QueryTime(),
		definer,
	), nil
}

func convertCreateEvent(ctx *sql.Context, query string, c *sqlparser.DDL) (sql.Node, error) {
	eventSpec := c.EventSpec
	udb, err := getUnresolvedDatabase(ctx, eventSpec.EventName.Qualifier.String())
	if err != nil {
		return nil, err
	}
	definer := getCurrentUserForDefiner(ctx, c.EventSpec.Definer)

	// both 'undefined' and 'not preserve' are considered 'not preserve'
	onCompletionPreserve := false
	if eventSpec.OnCompletionPreserve == sqlparser.EventOnCompletion_Preserve {
		onCompletionPreserve = true
	}

	var status plan.EventStatus
	switch eventSpec.Status {
	case sqlparser.EventStatus_Undefined:
		status = plan.EventStatus_Enable
	case sqlparser.EventStatus_Enable:
		status = plan.EventStatus_Enable
	case sqlparser.EventStatus_Disable:
		status = plan.EventStatus_Disable
	case sqlparser.EventStatus_DisableOnSlave:
		status = plan.EventStatus_DisableOnSlave
	}

	bodyStr := strings.TrimSpace(query[c.SubStatementPositionStart:c.SubStatementPositionEnd])
	body, err := convert(ctx, c.EventSpec.Body, bodyStr)
	if err != nil {
		return nil, err
	}

	var at, starts, ends *plan.OnScheduleTimestamp
	var everyInterval *expression.Interval
	if eventSpec.OnSchedule.At != nil {
		ts, intervals, err := convertEventScheduleTimeSpec(ctx, eventSpec.OnSchedule.At)
		if err != nil {
			return nil, err
		}
		at = plan.NewOnScheduleTimestamp(ts, intervals)
	} else {
		every, err := intervalExprToExpression(ctx, &eventSpec.OnSchedule.EveryInterval)
		if err != nil {
			return nil, err
		}

		var ok bool
		everyInterval, ok = every.(*expression.Interval)
		if !ok {
			return nil, fmt.Errorf("expected everyInterval but got: %s", every)
		}

		if eventSpec.OnSchedule.Starts != nil {
			startsTs, startsIntervals, err := convertEventScheduleTimeSpec(ctx, eventSpec.OnSchedule.Starts)
			if err != nil {
				return nil, err
			}
			starts = plan.NewOnScheduleTimestamp(startsTs, startsIntervals)
		}
		if eventSpec.OnSchedule.Ends != nil {
			endsTs, endsIntervals, err := convertEventScheduleTimeSpec(ctx, eventSpec.OnSchedule.Ends)
			if err != nil {
				return nil, err
			}
			ends = plan.NewOnScheduleTimestamp(endsTs, endsIntervals)
		}
	}

	comment := ""
	if eventSpec.Comment != nil {
		comment = string(eventSpec.Comment.Val)
	}

	return plan.NewCreateEvent(
		udb, eventSpec.EventName.Name.String(), definer,
		at, starts, ends, everyInterval,
		onCompletionPreserve,
		status, comment, bodyStr, body, eventSpec.IfNotExists,
	), nil
}

func convertEventScheduleTimeSpec(ctx *sql.Context, spec *sqlparser.EventScheduleTimeSpec) (sql.Expression, []sql.Expression, error) {
	ts, err := ExprToExpression(ctx, spec.EventTimestamp)
	if err != nil {
		return nil, nil, err
	}
	if len(spec.EventIntervals) == 0 {
		return ts, nil, nil
	}
	var intervals = make([]sql.Expression, len(spec.EventIntervals))
	for i, interval := range spec.EventIntervals {
		e, err := intervalExprToExpression(ctx, &interval)
		if err != nil {
			return nil, nil, err
		}
		intervals[i] = e
	}
	return ts, intervals, nil
}

func convertAlterEvent(ctx *sql.Context, query string, c *sqlparser.DDL) (sql.Node, error) {
	eventSpec := c.EventSpec
	udb, err := getUnresolvedDatabase(ctx, eventSpec.EventName.Qualifier.String())
	if err != nil {
		return nil, err
	}
	definer := getCurrentUserForDefiner(ctx, c.EventSpec.Definer)

	var (
		alterSchedule    = eventSpec.OnSchedule != nil
		at, starts, ends *plan.OnScheduleTimestamp
		everyInterval    *expression.Interval

		alterOnComp       = eventSpec.OnCompletionPreserve != sqlparser.EventOnCompletion_Undefined
		newOnCompPreserve = eventSpec.OnCompletionPreserve == sqlparser.EventOnCompletion_Preserve

		alterEventName = !eventSpec.RenameName.IsEmpty()
		newName        string

		alterStatus = eventSpec.Status != sqlparser.EventStatus_Undefined
		newStatus   plan.EventStatus

		alterComment = eventSpec.Comment != nil
		newComment   string

		alterDefinition  = eventSpec.Body != nil
		newDefinitionStr string
		newDefinition    sql.Node
	)

	if alterSchedule {
		if eventSpec.OnSchedule.At != nil {
			ts, intervals, err := convertEventScheduleTimeSpec(ctx, eventSpec.OnSchedule.At)
			if err != nil {
				return nil, err
			}
			at = plan.NewOnScheduleTimestamp(ts, intervals)
		} else {
			every, err := intervalExprToExpression(ctx, &eventSpec.OnSchedule.EveryInterval)
			if err != nil {
				return nil, err
			}

			var ok bool
			everyInterval, ok = every.(*expression.Interval)
			if !ok {
				return nil, fmt.Errorf("expected everyInterval but got: %s", every)
			}

			if eventSpec.OnSchedule.Starts != nil {
				startsTs, startsIntervals, err := convertEventScheduleTimeSpec(ctx, eventSpec.OnSchedule.Starts)
				if err != nil {
					return nil, err
				}
				starts = plan.NewOnScheduleTimestamp(startsTs, startsIntervals)
			}
			if eventSpec.OnSchedule.Ends != nil {
				endsTs, endsIntervals, err := convertEventScheduleTimeSpec(ctx, eventSpec.OnSchedule.Ends)
				if err != nil {
					return nil, err
				}
				ends = plan.NewOnScheduleTimestamp(endsTs, endsIntervals)
			}
		}
	}
	if alterEventName {
		// events can be moved to different database using RENAME TO clause option
		// TODO: we do not support moving events to different database yet
		renameEventDb := eventSpec.RenameName.Qualifier.String()
		if renameEventDb != "" && udb.Name() != renameEventDb {
			return nil, fmt.Errorf("moving events to different database using ALTER EVENT is not supported yet")
		}
		newName = eventSpec.RenameName.Name.String()
	}
	if alterStatus {
		switch eventSpec.Status {
		case sqlparser.EventStatus_Undefined:
			// this should not happen but sanity check
			newStatus = plan.EventStatus_Enable
		case sqlparser.EventStatus_Enable:
			newStatus = plan.EventStatus_Enable
		case sqlparser.EventStatus_Disable:
			newStatus = plan.EventStatus_Disable
		case sqlparser.EventStatus_DisableOnSlave:
			newStatus = plan.EventStatus_DisableOnSlave
		}
	}
	if alterComment {
		newComment = string(eventSpec.Comment.Val)
	}
	if alterDefinition {
		newDefinitionStr = strings.TrimSpace(query[c.SubStatementPositionStart:c.SubStatementPositionEnd])
		newDefinition, err = convert(ctx, c.EventSpec.Body, newDefinitionStr)
		if err != nil {
			return nil, err
		}
	}

	return plan.NewAlterEvent(
		udb, eventSpec.EventName.Name.String(), definer,
		alterSchedule, at, starts, ends, everyInterval,
		alterOnComp, newOnCompPreserve,
		alterEventName, newName,
		alterStatus, newStatus,
		alterComment, newComment,
		alterDefinition, newDefinitionStr, newDefinition,
	), nil
}

func convertCreateProcedure(ctx *sql.Context, query string, c *sqlparser.DDL) (sql.Node, error) {
	var params []plan.ProcedureParam
	for _, param := range c.ProcedureSpec.Params {
		var direction plan.ProcedureParamDirection
		switch param.Direction {
		case sqlparser.ProcedureParamDirection_In:
			direction = plan.ProcedureParamDirection_In
		case sqlparser.ProcedureParamDirection_Inout:
			direction = plan.ProcedureParamDirection_Inout
		case sqlparser.ProcedureParamDirection_Out:
			direction = plan.ProcedureParamDirection_Out
		default:
			return nil, fmt.Errorf("unknown procedure parameter direction: `%s`", string(param.Direction))
		}
		internalTyp, err := types.ColumnTypeToType(&param.Type)
		if err != nil {
			return nil, err
		}
		params = append(params, plan.ProcedureParam{
			Direction: direction,
			Name:      param.Name,
			Type:      internalTyp,
			Variadic:  false,
		})
	}

	var characteristics []plan.Characteristic
	securityType := plan.ProcedureSecurityContext_Definer // Default Security Context
	comment := ""
	for _, characteristic := range c.ProcedureSpec.Characteristics {
		switch characteristic.Type {
		case sqlparser.CharacteristicValue_Comment:
			comment = characteristic.Comment
		case sqlparser.CharacteristicValue_LanguageSql:
			characteristics = append(characteristics, plan.Characteristic_LanguageSql)
		case sqlparser.CharacteristicValue_Deterministic:
			characteristics = append(characteristics, plan.Characteristic_Deterministic)
		case sqlparser.CharacteristicValue_NotDeterministic:
			characteristics = append(characteristics, plan.Characteristic_NotDeterministic)
		case sqlparser.CharacteristicValue_ContainsSql:
			characteristics = append(characteristics, plan.Characteristic_ContainsSql)
		case sqlparser.CharacteristicValue_NoSql:
			characteristics = append(characteristics, plan.Characteristic_NoSql)
		case sqlparser.CharacteristicValue_ReadsSqlData:
			characteristics = append(characteristics, plan.Characteristic_ReadsSqlData)
		case sqlparser.CharacteristicValue_ModifiesSqlData:
			characteristics = append(characteristics, plan.Characteristic_ModifiesSqlData)
		case sqlparser.CharacteristicValue_SqlSecurityDefiner:
			// This is already the default value, so this prevents the default switch case
		case sqlparser.CharacteristicValue_SqlSecurityInvoker:
			securityType = plan.ProcedureSecurityContext_Invoker
		default:
			return nil, fmt.Errorf("unknown procedure characteristic: `%s`", string(characteristic.Type))
		}
	}

	bodyStr := strings.TrimSpace(query[c.SubStatementPositionStart:c.SubStatementPositionEnd])
	body, err := convert(ctx, c.ProcedureSpec.Body, bodyStr)
	if err != nil {
		return nil, err
	}

	return plan.NewCreateProcedure(
		sql.UnresolvedDatabase(c.ProcedureSpec.ProcName.Qualifier.String()),
		c.ProcedureSpec.ProcName.Name.String(),
		c.ProcedureSpec.Definer,
		params,
		time.Now(),
		time.Now(),
		securityType,
		characteristics,
		body,
		comment,
		query,
		bodyStr,
	), nil
}

func convertCall(ctx *sql.Context, c *sqlparser.Call) (sql.Node, error) {
	params := make([]sql.Expression, len(c.Params))
	for i, param := range c.Params {
		expr, err := ExprToExpression(ctx, param)
		if err != nil {
			return nil, err
		}
		params[i] = expr
	}

	var db sql.Database = nil
	if !c.ProcName.Qualifier.IsEmpty() {
		db = sql.UnresolvedDatabase(c.ProcName.Qualifier.String())
	}

	var asOf sql.Expression = nil
	if c.AsOf != nil {
		var err error
		asOf, err = ExprToExpression(ctx, c.AsOf)
		if err != nil {
			return nil, err
		}
	}

	return plan.NewCall(
		db,
		c.ProcName.Name.String(),
		params,
		asOf), nil
}

func convertDeclare(ctx *sql.Context, d *sqlparser.Declare, query string) (sql.Node, error) {
	if d.Condition != nil {
		return convertDeclareCondition(ctx, d)
	} else if d.Variables != nil {
		return convertDeclareVariables(ctx, d)
	} else if d.Cursor != nil {
		return convertDeclareCursor(ctx, d)
	} else if d.Handler != nil {
		return convertDeclareHandler(ctx, d, query)
	}
	return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(d))
}

func convertDeclareCondition(ctx *sql.Context, d *sqlparser.Declare) (sql.Node, error) {
	dc := d.Condition
	if dc.SqlStateValue != "" {
		if len(dc.SqlStateValue) != 5 {
			return nil, fmt.Errorf("SQLSTATE VALUE must be a string with length 5 consisting of only integers")
		}
		if dc.SqlStateValue[0:2] == "00" {
			return nil, fmt.Errorf("invalid SQLSTATE VALUE: '%s'", dc.SqlStateValue)
		}
	} else {
		number, err := strconv.ParseUint(string(dc.MysqlErrorCode.Val), 10, 64)
		if err != nil || number == 0 {
			// We use our own error instead
			return nil, fmt.Errorf("invalid value '%s' for MySQL error code", string(dc.MysqlErrorCode.Val))
		}
		//TODO: implement MySQL error code support
		return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(d))
	}
	return plan.NewDeclareCondition(strings.ToLower(dc.Name), 0, dc.SqlStateValue), nil
}

func convertDeclareVariables(ctx *sql.Context, d *sqlparser.Declare) (sql.Node, error) {
	dVars := d.Variables
	names := make([]string, len(dVars.Names))
	for i, variable := range dVars.Names {
		names[i] = variable.String()
	}
	typ, err := types.ColumnTypeToType(&dVars.VarType)
	if err != nil {
		return nil, err
	}
	defaultVal, err := convertDefaultExpression(ctx, dVars.VarType.Default)
	if err != nil {
		return nil, err
	}
	return plan.NewDeclareVariables(names, typ, defaultVal), nil
}

func convertDeclareCursor(ctx *sql.Context, d *sqlparser.Declare) (sql.Node, error) {
	dCursor := d.Cursor
	selectStmt, err := convertSelectStatement(ctx, dCursor.SelectStmt)
	if err != nil {
		return nil, err
	}
	return plan.NewDeclareCursor(dCursor.Name, selectStmt), nil
}

func convertDeclareHandler(ctx *sql.Context, d *sqlparser.Declare, query string) (sql.Node, error) {
	dHandler := d.Handler
	//TODO: support other condition values besides NOT FOUND
	if len(dHandler.ConditionValues) != 1 || dHandler.ConditionValues[0].ValueType != sqlparser.DeclareHandlerCondition_NotFound {
		return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(d))
	}
	stmt, err := convert(ctx, dHandler.Statement, query)
	if err != nil {
		return nil, err
	}

	var action plan.DeclareHandlerAction
	switch dHandler.Action {
	case sqlparser.DeclareHandlerAction_Continue:
		action = plan.DeclareHandlerAction_Continue
	case sqlparser.DeclareHandlerAction_Exit:
		action = plan.DeclareHandlerAction_Exit
	case sqlparser.DeclareHandlerAction_Undo:
		action = plan.DeclareHandlerAction_Undo
	default:
		return nil, fmt.Errorf("unknown DECLARE ... HANDLER action: %v", dHandler.Action)
	}
	return plan.NewDeclareHandler(action, stmt)
}

func convertFetch(ctx *sql.Context, fetchCursor *sqlparser.FetchCursor) (sql.Node, error) {
	return plan.NewFetch(fetchCursor.Name, fetchCursor.Variables), nil
}

func convertOpen(ctx *sql.Context, openCursor *sqlparser.OpenCursor) (sql.Node, error) {
	return plan.NewOpen(openCursor.Name), nil
}

func convertClose(ctx *sql.Context, closeCursor *sqlparser.CloseCursor) (sql.Node, error) {
	return plan.NewClose(closeCursor.Name), nil
}

func convertLoop(ctx *sql.Context, loop *sqlparser.Loop, query string) (sql.Node, error) {
	block, err := convertBlock(ctx, loop.Statements, query)
	if err != nil {
		return nil, err
	}
	return plan.NewLoop(loop.Label, block), nil
}

func convertRepeat(ctx *sql.Context, repeat *sqlparser.Repeat, query string) (sql.Node, error) {
	block, err := convertBlock(ctx, repeat.Statements, query)
	if err != nil {
		return nil, err
	}
	expr, err := ExprToExpression(ctx, repeat.Condition)
	if err != nil {
		return nil, err
	}
	return plan.NewRepeat(repeat.Label, expr, block), nil
}

func convertWhile(ctx *sql.Context, while *sqlparser.While, query string) (sql.Node, error) {
	block, err := convertBlock(ctx, while.Statements, query)
	if err != nil {
		return nil, err
	}
	expr, err := ExprToExpression(ctx, while.Condition)
	if err != nil {
		return nil, err
	}
	return plan.NewWhile(while.Label, expr, block), nil
}

func convertLeave(ctx *sql.Context, leave *sqlparser.Leave) (sql.Node, error) {
	return plan.NewLeave(leave.Label), nil
}

func convertIterate(ctx *sql.Context, iterate *sqlparser.Iterate) (sql.Node, error) {
	return plan.NewIterate(iterate.Label), nil
}

func convertSignal(ctx *sql.Context, s *sqlparser.Signal) (sql.Node, error) {
	// https://dev.mysql.com/doc/refman/8.0/en/signal.html#signal-condition-information-items
	var err error
	signalInfo := make(map[plan.SignalConditionItemName]plan.SignalInfo)
	for _, info := range s.Info {
		si := plan.SignalInfo{}
		si.ConditionItemName, err = convertSignalConditionItemName(info.ConditionItemName)
		if err != nil {
			return nil, err
		}
		if _, ok := signalInfo[si.ConditionItemName]; ok {
			return nil, fmt.Errorf("duplicate signal condition item")
		}

		if si.ConditionItemName == plan.SignalConditionItemName_MysqlErrno {
			switch v := info.Value.(type) {
			case *sqlparser.SQLVal:
				number, err := strconv.ParseUint(string(v.Val), 10, 16)
				if err != nil || number == 0 {
					// We use our own error instead
					return nil, fmt.Errorf("invalid value '%s' for signal condition information item MYSQL_ERRNO", string(v.Val))
				}
				si.IntValue = int64(number)
			default:
				return nil, fmt.Errorf("invalid value '%v' for signal condition information item MYSQL_ERRNO", info.Value)
			}
		} else if si.ConditionItemName == plan.SignalConditionItemName_MessageText {
			switch v := info.Value.(type) {
			case *sqlparser.SQLVal:
				val := string(v.Val)
				if len(val) > 128 {
					return nil, fmt.Errorf("signal condition information item MESSAGE_TEXT has max length of 128")
				}
				si.StrValue = val
			case *sqlparser.ColName:
				si.ExprVal = expression.NewUnresolvedColumn(v.Name.String())
			default:
				return nil, fmt.Errorf("invalid value '%v' for signal condition information item MESSAGE_TEXT", info.Value)
			}
		} else {
			switch v := info.Value.(type) {
			case *sqlparser.SQLVal:
				val := string(v.Val)
				if len(val) > 64 {
					return nil, fmt.Errorf("signal condition information item %s has max length of 64", strings.ToUpper(string(si.ConditionItemName)))
				}
				si.StrValue = val
			default:
				return nil, fmt.Errorf("invalid value '%v' for signal condition information item '%s''", info.Value, strings.ToUpper(string(si.ConditionItemName)))
			}
		}
		signalInfo[si.ConditionItemName] = si
	}

	if s.ConditionName != "" {
		return plan.NewSignalName(strings.ToLower(s.ConditionName), signalInfo), nil
	} else {
		if len(s.SqlStateValue) != 5 {
			return nil, fmt.Errorf("SQLSTATE VALUE must be a string with length 5 consisting of only integers")
		}
		if s.SqlStateValue[0:2] == "00" {
			return nil, fmt.Errorf("invalid SQLSTATE VALUE: '%s'", s.SqlStateValue)
		}
		return plan.NewSignal(s.SqlStateValue, signalInfo), nil
	}
}

func convertLockTables(ctx *sql.Context, s *sqlparser.LockTables) (sql.Node, error) {
	tables := make([]*plan.TableLock, len(s.Tables))

	for i, tbl := range s.Tables {
		tableNode, err := tableExprToTable(ctx, tbl.Table)
		if err != nil {
			return nil, err
		}

		write := tbl.Lock == sqlparser.LockWrite || tbl.Lock == sqlparser.LockLowPriorityWrite

		// TODO: Allow for other types of locks (LOW PRIORITY WRITE & LOCAL READ)
		tables[i] = &plan.TableLock{Table: tableNode, Write: write}
	}

	return plan.NewLockTables(tables), nil
}

func convertUnlockTables(ctx *sql.Context, s *sqlparser.UnlockTables) (sql.Node, error) {
	return plan.NewUnlockTables(), nil
}

func convertSignalConditionItemName(name sqlparser.SignalConditionItemName) (plan.SignalConditionItemName, error) {
	// We convert to our own plan equivalents to keep a separation between the parser and implementation
	switch name {
	case sqlparser.SignalConditionItemName_ClassOrigin:
		return plan.SignalConditionItemName_ClassOrigin, nil
	case sqlparser.SignalConditionItemName_SubclassOrigin:
		return plan.SignalConditionItemName_SubclassOrigin, nil
	case sqlparser.SignalConditionItemName_MessageText:
		return plan.SignalConditionItemName_MessageText, nil
	case sqlparser.SignalConditionItemName_MysqlErrno:
		return plan.SignalConditionItemName_MysqlErrno, nil
	case sqlparser.SignalConditionItemName_ConstraintCatalog:
		return plan.SignalConditionItemName_ConstraintCatalog, nil
	case sqlparser.SignalConditionItemName_ConstraintSchema:
		return plan.SignalConditionItemName_ConstraintSchema, nil
	case sqlparser.SignalConditionItemName_ConstraintName:
		return plan.SignalConditionItemName_ConstraintName, nil
	case sqlparser.SignalConditionItemName_CatalogName:
		return plan.SignalConditionItemName_CatalogName, nil
	case sqlparser.SignalConditionItemName_SchemaName:
		return plan.SignalConditionItemName_SchemaName, nil
	case sqlparser.SignalConditionItemName_TableName:
		return plan.SignalConditionItemName_TableName, nil
	case sqlparser.SignalConditionItemName_ColumnName:
		return plan.SignalConditionItemName_ColumnName, nil
	case sqlparser.SignalConditionItemName_CursorName:
		return plan.SignalConditionItemName_CursorName, nil
	default:
		return "", fmt.Errorf("unknown signal condition item name: %s", string(name))
	}
}

func convertRenameTable(ctx *sql.Context, ddl *sqlparser.DDL, alterTbl bool) (sql.Node, error) {
	if len(ddl.FromTables) != len(ddl.ToTables) {
		panic("Expected from tables and to tables of equal length")
	}

	var fromTables, toTables []string
	for _, table := range ddl.FromTables {
		fromTables = append(fromTables, table.Name.String())
	}
	for _, table := range ddl.ToTables {
		toTables = append(toTables, table.Name.String())
	}

	return plan.NewRenameTable(sql.UnresolvedDatabase(""), fromTables, toTables, alterTbl), nil
}

func isUniqueColumn(tableSpec *sqlparser.TableSpec, columnName string) (bool, error) {
	for _, column := range tableSpec.Columns {
		if column.Name.String() == columnName {
			return column.Type.KeyOpt == colKeyUnique ||
				column.Type.KeyOpt == colKeyUniqueKey, nil
		}
	}
	return false, fmt.Errorf("unknown column name %s", columnName)

}

func newColumnAction(ctx *sql.Context, ddl *sqlparser.DDL) (sql.Node, error) {
	switch strings.ToLower(ddl.ColumnAction) {
	case sqlparser.AddStr:
		sch, _, err := TableSpecToSchema(ctx, ddl.TableSpec, true)
		if err != nil {
			return nil, err
		}
		return plan.NewAddColumn(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), tableNameToUnresolvedTable(ddl.Table), sch.Schema[0], columnOrderToColumnOrder(ddl.ColumnOrder)), nil
	case sqlparser.DropStr:
		return plan.NewDropColumn(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), tableNameToUnresolvedTable(ddl.Table), ddl.Column.String()), nil
	case sqlparser.RenameStr:
		return plan.NewRenameColumn(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), tableNameToUnresolvedTable(ddl.Table), ddl.Column.String(), ddl.ToColumn.String()), nil
	case sqlparser.ModifyStr, sqlparser.ChangeStr:
		sch, _, err := TableSpecToSchema(ctx, ddl.TableSpec, true)
		if err != nil {
			return nil, err
		}
		return plan.NewModifyColumn(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), tableNameToUnresolvedTable(ddl.Table), ddl.Column.String(), sch.Schema[0], columnOrderToColumnOrder(ddl.ColumnOrder)), nil
	}
	return nil, sql.ErrUnsupportedFeature.New(sqlparser.String(ddl))
}

func convertAlterTable(ctx *sql.Context, ddl *sqlparser.DDL) (sql.Node, error) {
	if ddl.IndexSpec != nil {
		return convertAlterIndex(ctx, ddl)
	}
	if ddl.ConstraintAction != "" && len(ddl.TableSpec.Constraints) == 1 {
		table := tableNameToUnresolvedTable(ddl.Table)
		parsedConstraint, err := convertConstraintDefinition(ctx, ddl.TableSpec.Constraints[0])
		if err != nil {
			return nil, err
		}
		switch strings.ToLower(ddl.ConstraintAction) {
		case sqlparser.AddStr:
			switch c := parsedConstraint.(type) {
			case *sql.ForeignKeyConstraint:
				c.Database = table.Database().Name()
				c.Table = table.Name()
				if c.Database == "" {
					c.Database = ctx.GetCurrentDatabase()
				}
				return plan.NewAlterAddForeignKey(c), nil
			case *sql.CheckConstraint:
				return plan.NewAlterAddCheck(table, c), nil
			default:
				return nil, sql.ErrUnsupportedFeature.New(sqlparser.String(ddl))

			}
		case sqlparser.DropStr:
			switch c := parsedConstraint.(type) {
			case *sql.ForeignKeyConstraint:
				databaseName := table.Database().Name()
				if databaseName == "" {
					databaseName = ctx.GetCurrentDatabase()
				}
				return plan.NewAlterDropForeignKey(databaseName, table.Name(), c.Name), nil
			case *sql.CheckConstraint:
				return plan.NewAlterDropCheck(table, c.Name), nil
			case namedConstraint:
				return plan.NewDropConstraint(table, c.name), nil
			default:
				return nil, sql.ErrUnsupportedFeature.New(sqlparser.String(ddl))
			}
		}
	}

	if ddl.ColumnAction != "" {
		var alteredTable sql.Node
		alteredTable, err := newColumnAction(ctx, ddl)
		if err != nil {
			return nil, err
		}
		if ddl.TableSpec != nil {
			if len(ddl.TableSpec.Columns) != 1 {
				return nil, fmt.Errorf("adding multiple columns in ALTER TABLE <table> MODIFY is not currently supported")
			}
			for _, column := range ddl.TableSpec.Columns {
				isUnique, err := isUniqueColumn(ddl.TableSpec, column.Name.String())
				if err != nil {
					return nil, fmt.Errorf("on table %s, %w", ddl.Table.String(), err)
				}
				var comment string
				if commentVal := column.Type.Comment; commentVal != nil {
					comment = commentVal.String()
				}
				columns := []sql.IndexColumn{{Name: column.Name.String()}}
				if isUnique {
					alteredTable, err = plan.NewAlterCreateIndex(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), alteredTable, column.Name.String(), sql.IndexUsing_BTree, sql.IndexConstraint_Unique, columns, comment), nil
					if err != nil {
						return nil, err
					}
				}
			}
		}
		return alteredTable, nil
	}

	if ddl.AutoIncSpec != nil {
		return convertAlterAutoIncrement(ddl)
	}
	if ddl.DefaultSpec != nil {
		return convertAlterDefault(ctx, ddl)
	}
	if ddl.AlterCollationSpec != nil {
		return convertAlterCollationSpec(ctx, ddl)
	}
	return nil, sql.ErrUnsupportedFeature.New(sqlparser.String(ddl))
}

func tableNameToUnresolvedTable(tableName sqlparser.TableName) *plan.UnresolvedTable {
	return plan.NewUnresolvedTable(tableName.Name.String(), tableName.Qualifier.String())
}

func tableNameToUnresolvedTableAsOf(tableName sqlparser.TableName, asOf sql.Expression) *plan.UnresolvedTable {
	return plan.NewUnresolvedTableAsOf(tableName.Name.String(), tableName.Qualifier.String(), asOf)
}

func convertAlterIndex(ctx *sql.Context, ddl *sqlparser.DDL) (sql.Node, error) {
	table := tableNameToUnresolvedTable(ddl.Table)
	switch strings.ToLower(ddl.IndexSpec.Action) {
	case sqlparser.CreateStr:
		var using sql.IndexUsing
		switch ddl.IndexSpec.Using.Lowered() {
		case "", "btree":
			using = sql.IndexUsing_BTree
		case "hash":
			using = sql.IndexUsing_Hash
		default:
			return convertExternalCreateIndex(ctx, ddl)
		}

		var constraint sql.IndexConstraint
		switch ddl.IndexSpec.Type {
		case sqlparser.UniqueStr:
			constraint = sql.IndexConstraint_Unique
		case sqlparser.FulltextStr:
			return nil, sql.ErrUnsupportedFeature.New("fulltext keys are unsupported")
		case sqlparser.SpatialStr:
			constraint = sql.IndexConstraint_Spatial
		case sqlparser.PrimaryStr:
			constraint = sql.IndexConstraint_Primary
		default:
			constraint = sql.IndexConstraint_None
		}

		columns, err := gatherIndexColumns(ddl.IndexSpec.Columns)
		if err != nil {
			return nil, err
		}

		var comment string
		for _, option := range ddl.IndexSpec.Options {
			if strings.ToLower(option.Name) == strings.ToLower(sqlparser.KeywordString(sqlparser.COMMENT_KEYWORD)) {
				comment = string(option.Value.Val)
			}
		}

		if constraint == sql.IndexConstraint_Primary {
			return plan.NewAlterCreatePk(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), table, columns), nil
		}

		indexName := ddl.IndexSpec.ToName.String()
		if strings.ToLower(indexName) == sqlparser.PrimaryStr {
			return nil, errIncorrectIndexName.New(indexName)
		}

		return plan.NewAlterCreateIndex(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), table, indexName, using, constraint, columns, comment), nil
	case sqlparser.DropStr:
		if ddl.IndexSpec.Type == sqlparser.PrimaryStr {
			return plan.NewAlterDropPk(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), table), nil
		}
		return plan.NewAlterDropIndex(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), table, ddl.IndexSpec.ToName.String()), nil
	case sqlparser.RenameStr:
		return plan.NewAlterRenameIndex(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), table, ddl.IndexSpec.FromName.String(), ddl.IndexSpec.ToName.String()), nil
	case "disable":
		return plan.NewAlterDisableEnableKeys(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), table, true), nil
	case "enable":
		return plan.NewAlterDisableEnableKeys(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), table, false), nil
	default:
		return nil, sql.ErrUnsupportedFeature.New(sqlparser.String(ddl))
	}
}

func gatherIndexColumns(cols []*sqlparser.IndexColumn) ([]sql.IndexColumn, error) {
	out := make([]sql.IndexColumn, len(cols))
	for i, col := range cols {
		var length int64
		var err error
		if col.Length != nil && col.Length.Type == sqlparser.IntVal {
			length, err = strconv.ParseInt(string(col.Length.Val), 10, 64)
			if err != nil {
				return nil, err
			}
			if length < 1 {
				return nil, sql.ErrKeyZero.New(col.Column)
			}
		}
		out[i] = sql.IndexColumn{
			Name:   col.Column.String(),
			Length: length,
		}
	}
	return out, nil
}

func convertAlterAutoIncrement(ddl *sqlparser.DDL) (sql.Node, error) {
	val, ok := ddl.AutoIncSpec.Value.(*sqlparser.SQLVal)
	if !ok {
		return nil, sql.ErrInvalidSQLValType.New(ddl.AutoIncSpec.Value)
	}

	var autoVal uint64
	if val.Type == sqlparser.IntVal {
		i, err := strconv.ParseUint(string(val.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		autoVal = i
	} else if val.Type == sqlparser.FloatVal {
		f, err := strconv.ParseFloat(string(val.Val), 10)
		if err != nil {
			return nil, err
		}
		autoVal = uint64(f)
	} else {
		return nil, sql.ErrInvalidSQLValType.New(ddl.AutoIncSpec.Value)
	}

	return plan.NewAlterAutoIncrement(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), tableNameToUnresolvedTable(ddl.Table), autoVal), nil
}

func convertAlterDefault(ctx *sql.Context, ddl *sqlparser.DDL) (sql.Node, error) {
	table := tableNameToUnresolvedTable(ddl.Table)
	switch strings.ToLower(ddl.DefaultSpec.Action) {
	case sqlparser.SetStr:
		defaultVal, err := convertDefaultExpression(ctx, ddl.DefaultSpec.Value)
		if err != nil {
			return nil, err
		}
		return plan.NewAlterDefaultSet(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), table, ddl.DefaultSpec.Column.String(), defaultVal), nil
	case sqlparser.DropStr:
		return plan.NewAlterDefaultDrop(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), table, ddl.DefaultSpec.Column.String()), nil
	default:
		return nil, sql.ErrUnsupportedFeature.New(sqlparser.String(ddl))
	}
}

func convertAlterCollationSpec(ctx *sql.Context, ddl *sqlparser.DDL) (sql.Node, error) {
	table := tableNameToUnresolvedTable(ddl.Table)
	var charSetStr *string
	var collationStr *string
	if len(ddl.AlterCollationSpec.CharacterSet) > 0 {
		charSetStr = &ddl.AlterCollationSpec.CharacterSet
	}
	if len(ddl.AlterCollationSpec.Collation) > 0 {
		collationStr = &ddl.AlterCollationSpec.Collation
	}
	collation, err := sql.ParseCollation(charSetStr, collationStr, false)
	if err != nil {
		return nil, err
	}
	return plan.NewAlterTableCollation(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), table, collation), nil
}

func convertExternalCreateIndex(ctx *sql.Context, ddl *sqlparser.DDL) (sql.Node, error) {
	config := make(map[string]string)
	for _, option := range ddl.IndexSpec.Options {
		if option.Using != "" {
			config[option.Name] = option.Using
		} else {
			config[option.Name] = string(option.Value.Val)
		}
	}
	cols := make([]sql.Expression, len(ddl.IndexSpec.Columns))
	for i, col := range ddl.IndexSpec.Columns {
		cols[i] = expression.NewUnresolvedColumn(col.Column.String())
	}
	return plan.NewCreateIndex(
		ddl.IndexSpec.ToName.String(),
		tableNameToUnresolvedTable(ddl.Table),
		cols,
		ddl.IndexSpec.Using.Lowered(),
		config,
	), nil
}

func columnOrderToColumnOrder(order *sqlparser.ColumnOrder) *sql.ColumnOrder {
	if order == nil {
		return nil
	}
	if order.First {
		return &sql.ColumnOrder{First: true}
	} else {
		return &sql.ColumnOrder{AfterColumn: order.AfterColumn.String()}
	}
}

func convertDropTable(ctx *sql.Context, c *sqlparser.DDL) (sql.Node, error) {
	dropTables := make([]sql.Node, len(c.FromTables))
	dbName := c.FromTables[0].Qualifier.String()
	for i, t := range c.FromTables {
		if t.Qualifier.String() != dbName {
			return nil, sql.ErrUnsupportedFeature.New("dropping tables on multiple databases in the same statement")
		}
		dropTables[i] = tableNameToUnresolvedTable(t)
	}

	return plan.NewDropTable(dropTables, c.IfExists), nil
}

func convertTruncateTable(ctx *sql.Context, c *sqlparser.DDL) (sql.Node, error) {
	return plan.NewTruncate(
		c.Table.Qualifier.String(),
		tableNameToUnresolvedTable(c.Table),
	), nil
}

func convertCreateTable(ctx *sql.Context, c *sqlparser.DDL) (sql.Node, error) {
	if c.OptLike != nil {
		return plan.NewCreateTableLike(
			sql.UnresolvedDatabase(""),
			c.Table.Name.String(),
			plan.NewUnresolvedTable(c.OptLike.LikeTable.Name.String(), c.OptLike.LikeTable.Qualifier.String()),
			plan.IfNotExistsOption(c.IfNotExists),
			plan.TempTableOption(c.Temporary),
		), nil
	}

	// In the case that no table spec is given but a SELECT Statement return the CREATE TABLE noder.
	// if the table spec != nil it will get parsed below.
	if c.TableSpec == nil && c.OptSelect != nil {
		tableSpec := &plan.TableSpec{}

		selectNode, err := convertSelectStatement(ctx, c.OptSelect.Select)
		if err != nil {
			return nil, err
		}

		return plan.NewCreateTableSelect(sql.UnresolvedDatabase(c.Table.Qualifier.String()), c.Table.Name.String(), selectNode, tableSpec, plan.IfNotExistsOption(c.IfNotExists), plan.TempTableOption(c.Temporary)), nil
	}

	fkDefs, chDefs, err := ConvertConstraintsDefs(ctx, c.Table, c.TableSpec)
	if err != nil {
		return nil, err
	}

	idxDefs, err := ConvertIndexDefs(ctx, c.TableSpec)
	if err != nil {
		return nil, err
	}

	qualifier := c.Table.Qualifier.String()

	schema, collation, err := TableSpecToSchema(ctx, c.TableSpec, false)
	if err != nil {
		return nil, err
	}

	tableSpec := &plan.TableSpec{
		Schema:    schema,
		IdxDefs:   idxDefs,
		FkDefs:    fkDefs,
		ChDefs:    chDefs,
		Collation: collation,
	}

	if c.OptSelect != nil {
		selectNode, err := convertSelectStatement(ctx, c.OptSelect.Select)
		if err != nil {
			return nil, err
		}

		return plan.NewCreateTableSelect(sql.UnresolvedDatabase(qualifier), c.Table.Name.String(), selectNode, tableSpec, plan.IfNotExistsOption(c.IfNotExists), plan.TempTableOption(c.Temporary)), nil
	}

	return plan.NewCreateTable(
		sql.UnresolvedDatabase(qualifier), c.Table.Name.String(), plan.IfNotExistsOption(c.IfNotExists), plan.TempTableOption(c.Temporary), tableSpec), nil
}

func ConvertConstraintsDefs(ctx *sql.Context, tname sqlparser.TableName, spec *sqlparser.TableSpec) (fks []*sql.ForeignKeyConstraint, checks []*sql.CheckConstraint, err error) {
	for _, unknownConstraint := range spec.Constraints {
		parsedConstraint, err := convertConstraintDefinition(ctx, unknownConstraint)
		if err != nil {
			return nil, nil, err
		}
		switch constraint := parsedConstraint.(type) {
		case *sql.ForeignKeyConstraint:
			constraint.Database = tname.Qualifier.String()
			constraint.Table = tname.Name.String()
			if constraint.Database == "" {
				constraint.Database = ctx.GetCurrentDatabase()
			}
			fks = append(fks, constraint)
		case *sql.CheckConstraint:
			checks = append(checks, constraint)
		default:
			return nil, nil, sql.ErrUnknownConstraintDefinition.New(unknownConstraint.Name, unknownConstraint)
		}
	}
	return
}

func ConvertIndexDefs(ctx *sql.Context, spec *sqlparser.TableSpec) (idxDefs []*plan.IndexDefinition, err error) {
	for _, idxDef := range spec.Indexes {
		constraint := sql.IndexConstraint_None
		if idxDef.Info.Primary {
			constraint = sql.IndexConstraint_Primary
		} else if idxDef.Info.Unique {
			constraint = sql.IndexConstraint_Unique
		} else if idxDef.Info.Spatial {
			constraint = sql.IndexConstraint_Spatial
		} else if idxDef.Info.Fulltext {
			// TODO: We do not support FULLTEXT indexes or keys
			ctx.Warn(1214, "ignoring fulltext index as they have not yet been implemented")
			continue
		}

		columns, err := gatherIndexColumns(idxDef.Columns)
		if err != nil {
			return nil, err
		}

		var comment string
		for _, option := range idxDef.Options {
			if strings.ToLower(option.Name) == strings.ToLower(sqlparser.KeywordString(sqlparser.COMMENT_KEYWORD)) {
				comment = string(option.Value.Val)
			}
		}
		idxDefs = append(idxDefs, &plan.IndexDefinition{
			IndexName:  idxDef.Info.Name.String(),
			Using:      sql.IndexUsing_Default, //TODO: add vitess support for USING
			Constraint: constraint,
			Columns:    columns,
			Comment:    comment,
		})
	}

	for _, colDef := range spec.Columns {
		if colDef.Type.KeyOpt == colKeyFulltextKey {
			// TODO: We do not support FULLTEXT indexes or keys
			ctx.Warn(1214, "ignoring fulltext index as they have not yet been implemented")
			continue
		}
		if colDef.Type.KeyOpt == colKeyUnique || colDef.Type.KeyOpt == colKeyUniqueKey {
			idxDefs = append(idxDefs, &plan.IndexDefinition{
				IndexName:  "",
				Using:      sql.IndexUsing_Default,
				Constraint: sql.IndexConstraint_Unique,
				Comment:    "",
				Columns: []sql.IndexColumn{{
					Name:   colDef.Name.String(),
					Length: 0,
				}},
			})
		}
	}
	return
}

type namedConstraint struct {
	name string
}

func convertConstraintDefinition(ctx *sql.Context, cd *sqlparser.ConstraintDefinition) (interface{}, error) {
	if fkConstraint, ok := cd.Details.(*sqlparser.ForeignKeyDefinition); ok {
		columns := make([]string, len(fkConstraint.Source))
		for i, col := range fkConstraint.Source {
			columns[i] = col.String()
		}
		refColumns := make([]string, len(fkConstraint.ReferencedColumns))
		for i, col := range fkConstraint.ReferencedColumns {
			refColumns[i] = col.String()
		}
		refDatabase := fkConstraint.ReferencedTable.Qualifier.String()
		if refDatabase == "" {
			refDatabase = ctx.GetCurrentDatabase()
		}
		// The database and table are set in the calling function
		return &sql.ForeignKeyConstraint{
			Name:           cd.Name,
			Columns:        columns,
			ParentDatabase: refDatabase,
			ParentTable:    fkConstraint.ReferencedTable.Name.String(),
			ParentColumns:  refColumns,
			OnUpdate:       convertReferentialAction(fkConstraint.OnUpdate),
			OnDelete:       convertReferentialAction(fkConstraint.OnDelete),
			IsResolved:     false,
		}, nil
	} else if chConstraint, ok := cd.Details.(*sqlparser.CheckConstraintDefinition); ok {
		var c sql.Expression
		var err error
		if chConstraint.Expr != nil {
			c, err = ExprToExpression(ctx, chConstraint.Expr)
			if err != nil {
				return nil, err
			}
		}

		return &sql.CheckConstraint{
			Name:     cd.Name,
			Expr:     c,
			Enforced: chConstraint.Enforced,
		}, nil
	} else if len(cd.Name) > 0 && cd.Details == nil {
		return namedConstraint{cd.Name}, nil
	}
	return nil, sql.ErrUnknownConstraintDefinition.New(cd.Name, cd)
}

func convertReferentialAction(action sqlparser.ReferenceAction) sql.ForeignKeyReferentialAction {
	switch action {
	case sqlparser.Restrict:
		return sql.ForeignKeyReferentialAction_Restrict
	case sqlparser.Cascade:
		return sql.ForeignKeyReferentialAction_Cascade
	case sqlparser.NoAction:
		return sql.ForeignKeyReferentialAction_NoAction
	case sqlparser.SetNull:
		return sql.ForeignKeyReferentialAction_SetNull
	case sqlparser.SetDefault:
		return sql.ForeignKeyReferentialAction_SetDefault
	default:
		return sql.ForeignKeyReferentialAction_DefaultAction
	}
}

func convertCreateView(ctx *sql.Context, query string, c *sqlparser.DDL) (sql.Node, error) {
	selectStatement, ok := c.ViewSpec.ViewExpr.(sqlparser.SelectStatement)
	if !ok {
		return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(c.ViewSpec.ViewExpr))
	}

	queryNode, err := convertSelectStatement(ctx, selectStatement)
	if err != nil {
		return nil, err
	}

	selectStr := query[c.SubStatementPositionStart:c.SubStatementPositionEnd]
	queryAlias := plan.NewSubqueryAlias(c.ViewSpec.ViewName.Name.String(), selectStr, queryNode)
	definer := getCurrentUserForDefiner(ctx, c.ViewSpec.Definer)

	return plan.NewCreateView(
		sql.UnresolvedDatabase(""), c.ViewSpec.ViewName.Name.String(), []string{}, queryAlias, c.OrReplace, query, c.ViewSpec.Algorithm, definer, c.ViewSpec.Security), nil
}

func convertDropView(ctx *sql.Context, c *sqlparser.DDL) (sql.Node, error) {
	plans := make([]sql.Node, len(c.FromViews))
	for i, v := range c.FromViews {
		plans[i] = plan.NewSingleDropView(sql.UnresolvedDatabase(""), v.Name.String())
	}
	return plan.NewDropView(plans, c.IfExists), nil
}

func convertInsert(ctx *sql.Context, i *sqlparser.Insert) (sql.Node, error) {
	onDupExprs, err := assignmentExprsToExpressions(ctx, sqlparser.AssignmentExprs(i.OnDup))
	if err != nil {
		return nil, err
	}

	isReplace := i.Action == sqlparser.ReplaceStr

	src, err := insertRowsToNode(ctx, i.Rows)
	if err != nil {
		return nil, err
	}

	ignore := false
	// TODO: make this a bool in vitess
	if strings.Contains(strings.ToLower(i.Ignore), "ignore") {
		ignore = true
	}

	var columns = columnsToStrings(i.Columns)

	var node sql.Node
	node, err = plan.NewInsertInto(sql.UnresolvedDatabase(i.Table.Qualifier.String()), tableNameToUnresolvedTable(i.Table), src, isReplace, columns, onDupExprs, ignore), nil
	if err != nil {
		return nil, err
	}

	if i.With != nil {
		node, err = ctesToWith(ctx, i.With, node)
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

func convertDelete(ctx *sql.Context, d *sqlparser.Delete) (sql.Node, error) {
	node, err := tableExprsToTable(ctx, d.TableExprs)
	if err != nil {
		return nil, err
	}

	if d.Where != nil {
		node, err = whereToFilter(ctx, d.Where, node)
		if err != nil {
			return nil, err
		}
	}

	if len(d.OrderBy) != 0 {
		node, err = orderByToSort(ctx, d.OrderBy, node)
		if err != nil {
			return nil, err
		}
	}

	// Limit must wrap offset, and not vice-versa, so that skipped rows don't count toward the returned row count.
	if d.Limit != nil && d.Limit.Offset != nil {
		node, err = offsetToOffset(ctx, d.Limit.Offset, node)
		if err != nil {
			return nil, err
		}
	}

	if d.Limit != nil {
		node, err = limitToLimit(ctx, d.Limit.Rowcount, node)
		if err != nil {
			return nil, err
		}
	}

	var targets []sql.Node
	if len(d.Targets) > 0 {
		targets = make([]sql.Node, len(d.Targets))
		for i, tableName := range d.Targets {
			targets[i] = tableNameToUnresolvedTable(tableName)
		}
	}

	node = plan.NewDeleteFrom(node, targets)

	if d.With != nil {
		node, err = ctesToWith(ctx, d.With, node)
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

func convertUpdate(ctx *sql.Context, u *sqlparser.Update) (sql.Node, error) {
	node, err := tableExprsToTable(ctx, u.TableExprs)
	if err != nil {
		return nil, err
	}

	// If the top level node can store comments and one was provided, store it.
	if cn, ok := node.(sql.CommentedNode); ok && len(u.Comments) > 0 {
		node = cn.WithComment(string(u.Comments[0]))
	}

	updateExprs, err := assignmentExprsToExpressions(ctx, u.Exprs)
	if err != nil {
		return nil, err
	}

	if u.Where != nil {
		node, err = whereToFilter(ctx, u.Where, node)
		if err != nil {
			return nil, err
		}
	}

	if len(u.OrderBy) != 0 {
		node, err = orderByToSort(ctx, u.OrderBy, node)
		if err != nil {
			return nil, err
		}
	}

	// Limit must wrap offset, and not vice-versa, so that skipped rows don't count toward the returned row count.
	if u.Limit != nil && u.Limit.Offset != nil {
		node, err = offsetToOffset(ctx, u.Limit.Offset, node)
		if err != nil {
			return nil, err
		}

	}

	if u.Limit != nil {
		node, err = limitToLimit(ctx, u.Limit.Rowcount, node)
		if err != nil {
			return nil, err
		}
	}

	ignore := u.Ignore != ""

	node, err = plan.NewUpdate(node, ignore, updateExprs), nil
	if err != nil {
		return nil, err
	}

	if u.With != nil {
		node, err = ctesToWith(ctx, u.With, node)
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

func convertLoad(ctx *sql.Context, d *sqlparser.Load) (sql.Node, error) {
	unresolvedTable := tableNameToUnresolvedTable(d.Table)

	var ignoreNumVal int64 = 0
	var err error
	if d.IgnoreNum != nil {
		ignoreNumVal, err = getInt64Value(ctx, d.IgnoreNum, "Cannot parse ignore Value")
		if err != nil {
			return nil, err
		}
	}

	ld := plan.NewLoadData(bool(d.Local), d.Infile, unresolvedTable, columnsToStrings(d.Columns), d.Fields, d.Lines, ignoreNumVal, d.IgnoreOrReplace)

	return plan.NewInsertInto(sql.UnresolvedDatabase(d.Table.Qualifier.String()), tableNameToUnresolvedTable(d.Table), ld, ld.IsReplace, ld.ColumnNames, nil, ld.IsIgnore), nil
}

func getPkOrdinals(ts *sqlparser.TableSpec) []int {
	for _, idxDef := range ts.Indexes {
		if idxDef.Info.Primary {

			pkOrdinals := make([]int, 0)
			colIdx := make(map[string]int)
			for i := 0; i < len(ts.Columns); i++ {
				colIdx[ts.Columns[i].Name.Lowered()] = i
			}

			for _, i := range idxDef.Columns {
				pkOrdinals = append(pkOrdinals, colIdx[i.Column.Lowered()])
			}

			return pkOrdinals
		}
	}

	// no primary key expression, check for inline PK column
	for i, col := range ts.Columns {
		if col.Type.KeyOpt == colKeyPrimary {
			return []int{i}
		}
	}

	return []int{}
}

// TableSpecToSchema creates a sql.Schema from a parsed TableSpec
func TableSpecToSchema(ctx *sql.Context, tableSpec *sqlparser.TableSpec, forceInvalidCollation bool) (sql.PrimaryKeySchema, sql.CollationID, error) {
	tableCollation := sql.Collation_Unspecified
	if !forceInvalidCollation {
		if len(tableSpec.Options) > 0 {
			charsetSubmatches := tableCharsetOptionRegex.FindStringSubmatch(tableSpec.Options)
			collationSubmatches := tableCollationOptionRegex.FindStringSubmatch(tableSpec.Options)
			if len(charsetSubmatches) == 5 && len(collationSubmatches) == 5 {
				var err error
				tableCollation, err = sql.ParseCollation(&charsetSubmatches[4], &collationSubmatches[4], false)
				if err != nil {
					return sql.PrimaryKeySchema{}, sql.Collation_Unspecified, err
				}
			} else if len(charsetSubmatches) == 5 {
				charset, err := sql.ParseCharacterSet(charsetSubmatches[4])
				if err != nil {
					return sql.PrimaryKeySchema{}, sql.Collation_Unspecified, err
				}
				tableCollation = charset.DefaultCollation()
			} else if len(collationSubmatches) == 5 {
				var err error
				tableCollation, err = sql.ParseCollation(nil, &collationSubmatches[4], false)
				if err != nil {
					return sql.PrimaryKeySchema{}, sql.Collation_Unspecified, err
				}
			}
		}
	}

	var schema sql.Schema
	for _, cd := range tableSpec.Columns {
		// Use the table's collation if no character or collation was specified for the table
		if len(cd.Type.Charset) == 0 && len(cd.Type.Collate) == 0 {
			if tableCollation != sql.Collation_Unspecified {
				cd.Type.Collate = tableCollation.Name()
			}
		}
		column, err := columnDefinitionToColumn(ctx, cd, tableSpec.Indexes)
		if err != nil {
			return sql.PrimaryKeySchema{}, sql.Collation_Unspecified, err
		}

		if column.PrimaryKey && bool(cd.Type.Null) {
			return sql.PrimaryKeySchema{}, sql.Collation_Unspecified, ErrPrimaryKeyOnNullField.New()
		}

		schema = append(schema, column)
	}

	return sql.NewPrimaryKeySchema(schema, getPkOrdinals(tableSpec)...), tableCollation, nil
}

// columnDefinitionToColumn returns the sql.Column for the column definition given, as part of a create table statement.
func columnDefinitionToColumn(ctx *sql.Context, cd *sqlparser.ColumnDefinition, indexes []*sqlparser.IndexDefinition) (*sql.Column, error) {
	internalTyp, err := types.ColumnTypeToType(&cd.Type)
	if err != nil {
		return nil, err
	}

	// Primary key info can either be specified in the column's type info (for in-line declarations), or in a slice of
	// indexes attached to the table def. We have to check both places to find if a column is part of the primary key
	isPkey := cd.Type.KeyOpt == colKeyPrimary

	if !isPkey {
	OuterLoop:
		for _, index := range indexes {
			if index.Info.Primary {
				for _, indexCol := range index.Columns {
					if indexCol.Column.Equal(cd.Name) {
						isPkey = true
						break OuterLoop
					}
				}
			}
		}
	}

	var comment string
	if cd.Type.Comment != nil && cd.Type.Comment.Type == sqlparser.StrVal {
		comment = string(cd.Type.Comment.Val)
	}

	defaultVal, err := convertDefaultExpression(ctx, cd.Type.Default)
	if err != nil {
		return nil, err
	}

	extra := ""

	if cd.Type.Autoincrement {
		extra = "auto_increment"
	}

	if cd.Type.SRID != nil {
		sridVal, sErr := strconv.ParseInt(string(cd.Type.SRID.Val), 10, 32)
		if sErr != nil {
			return nil, sErr
		}
		if err = types.ValidateSRID(int(sridVal), ""); err != nil {
			return nil, err
		}
		if s, ok := internalTyp.(sql.SpatialColumnType); ok {
			internalTyp = s.SetSRID(uint32(sridVal))
		} else {
			return nil, sql.ErrInvalidType.New(fmt.Sprintf("cannot define SRID for %s", internalTyp))
		}
	}

	return &sql.Column{
		Nullable:      !isPkey && !bool(cd.Type.NotNull),
		Type:          internalTyp,
		Name:          cd.Name.String(),
		PrimaryKey:    isPkey,
		Default:       defaultVal,
		AutoIncrement: bool(cd.Type.Autoincrement),
		Comment:       comment,
		Extra:         extra,
	}, nil
}

func convertDefaultExpression(ctx *sql.Context, defaultExpr sqlparser.Expr) (*sql.ColumnDefaultValue, error) {
	if defaultExpr == nil {
		return nil, nil
	}
	parsedExpr, err := ExprToExpression(ctx, defaultExpr)
	if err != nil {
		return nil, err
	}

	// Function expressions must be enclosed in parentheses (except for current_timestamp() and now())
	_, isParenthesized := defaultExpr.(*sqlparser.ParenExpr)
	isLiteral := !isParenthesized

	// A literal will never have children, thus we can also check for that.
	if unaryExpr, is := defaultExpr.(*sqlparser.UnaryExpr); is {
		if _, lit := unaryExpr.Expr.(*sqlparser.SQLVal); lit {
			isLiteral = true
		}
	} else if !isParenthesized {
		if f, ok := parsedExpr.(*expression.UnresolvedFunction); ok {
			// Datetime and Timestamp columns allow now and current_timestamp to not be enclosed in parens,
			// but they still need to be treated as function expressions
			if f.Name() == "now" || f.Name() == "current_timestamp" {
				isLiteral = false
			} else {
				// All other functions must *always* be enclosed in parens
				return nil, sql.ErrSyntaxError.New("column default function expressions must be enclosed in parentheses")
			}
		}
	}

	return ExpressionToColumnDefaultValue(ctx, parsedExpr, isLiteral, isParenthesized)
}

func convertAccountName(names ...sqlparser.AccountName) []plan.UserName {
	userNames := make([]plan.UserName, len(names))
	for i, name := range names {
		userNames[i] = plan.UserName{
			Name:    name.Name,
			Host:    name.Host,
			AnyHost: name.AnyHost,
		}
	}
	return userNames
}

func convertPrivilege(privileges ...sqlparser.Privilege) []plan.Privilege {
	planPrivs := make([]plan.Privilege, len(privileges))
	for i, privilege := range privileges {
		var privType plan.PrivilegeType
		var dynamicString string
		switch privilege.Type {
		case sqlparser.PrivilegeType_All:
			privType = plan.PrivilegeType_All
		case sqlparser.PrivilegeType_Alter:
			privType = plan.PrivilegeType_Alter
		case sqlparser.PrivilegeType_AlterRoutine:
			privType = plan.PrivilegeType_AlterRoutine
		case sqlparser.PrivilegeType_Create:
			privType = plan.PrivilegeType_Create
		case sqlparser.PrivilegeType_CreateRole:
			privType = plan.PrivilegeType_CreateRole
		case sqlparser.PrivilegeType_CreateRoutine:
			privType = plan.PrivilegeType_CreateRoutine
		case sqlparser.PrivilegeType_CreateTablespace:
			privType = plan.PrivilegeType_CreateTablespace
		case sqlparser.PrivilegeType_CreateTemporaryTables:
			privType = plan.PrivilegeType_CreateTemporaryTables
		case sqlparser.PrivilegeType_CreateUser:
			privType = plan.PrivilegeType_CreateUser
		case sqlparser.PrivilegeType_CreateView:
			privType = plan.PrivilegeType_CreateView
		case sqlparser.PrivilegeType_Delete:
			privType = plan.PrivilegeType_Delete
		case sqlparser.PrivilegeType_Drop:
			privType = plan.PrivilegeType_Drop
		case sqlparser.PrivilegeType_DropRole:
			privType = plan.PrivilegeType_DropRole
		case sqlparser.PrivilegeType_Event:
			privType = plan.PrivilegeType_Event
		case sqlparser.PrivilegeType_Execute:
			privType = plan.PrivilegeType_Execute
		case sqlparser.PrivilegeType_File:
			privType = plan.PrivilegeType_File
		case sqlparser.PrivilegeType_GrantOption:
			privType = plan.PrivilegeType_GrantOption
		case sqlparser.PrivilegeType_Index:
			privType = plan.PrivilegeType_Index
		case sqlparser.PrivilegeType_Insert:
			privType = plan.PrivilegeType_Insert
		case sqlparser.PrivilegeType_LockTables:
			privType = plan.PrivilegeType_LockTables
		case sqlparser.PrivilegeType_Process:
			privType = plan.PrivilegeType_Process
		case sqlparser.PrivilegeType_References:
			privType = plan.PrivilegeType_References
		case sqlparser.PrivilegeType_Reload:
			privType = plan.PrivilegeType_Reload
		case sqlparser.PrivilegeType_ReplicationClient:
			privType = plan.PrivilegeType_ReplicationClient
		case sqlparser.PrivilegeType_ReplicationSlave:
			privType = plan.PrivilegeType_ReplicationSlave
		case sqlparser.PrivilegeType_Select:
			privType = plan.PrivilegeType_Select
		case sqlparser.PrivilegeType_ShowDatabases:
			privType = plan.PrivilegeType_ShowDatabases
		case sqlparser.PrivilegeType_ShowView:
			privType = plan.PrivilegeType_ShowView
		case sqlparser.PrivilegeType_Shutdown:
			privType = plan.PrivilegeType_Shutdown
		case sqlparser.PrivilegeType_Super:
			privType = plan.PrivilegeType_Super
		case sqlparser.PrivilegeType_Trigger:
			privType = plan.PrivilegeType_Trigger
		case sqlparser.PrivilegeType_Update:
			privType = plan.PrivilegeType_Update
		case sqlparser.PrivilegeType_Usage:
			privType = plan.PrivilegeType_Usage
		case sqlparser.PrivilegeType_Dynamic:
			privType = plan.PrivilegeType_Dynamic
			dynamicString = privilege.DynamicName
		default:
			// all privileges have been implemented, so if we hit the default something bad has happened
			panic("given privilege type parses but is not implemented")
		}
		planPrivs[i] = plan.Privilege{
			Type:    privType,
			Columns: privilege.Columns,
			Dynamic: dynamicString,
		}
	}
	return planPrivs
}

func convertObjectType(objType sqlparser.GrantObjectType) plan.ObjectType {
	switch objType {
	case sqlparser.GrantObjectType_Any:
		return plan.ObjectType_Any
	case sqlparser.GrantObjectType_Table:
		return plan.ObjectType_Table
	case sqlparser.GrantObjectType_Function:
		return plan.ObjectType_Function
	case sqlparser.GrantObjectType_Procedure:
		return plan.ObjectType_Procedure
	default:
		panic("no other grant object types exist")
	}
}

func convertPrivilegeLevel(privLevel sqlparser.PrivilegeLevel) plan.PrivilegeLevel {
	return plan.PrivilegeLevel{
		Database:     privLevel.Database,
		TableRoutine: privLevel.TableRoutine,
	}
}

func convertCreateUser(ctx *sql.Context, n *sqlparser.CreateUser) (*plan.CreateUser, error) {
	authUsers := make([]plan.AuthenticatedUser, len(n.Users))
	for i, user := range n.Users {
		authUser := plan.AuthenticatedUser{
			UserName: convertAccountName(user.AccountName)[0],
		}
		if user.Auth1 != nil {
			authUser.Identity = user.Auth1.Identity
			if user.Auth1.Plugin == "mysql_native_password" && len(user.Auth1.Password) > 0 {
				authUser.Auth1 = plan.AuthenticationMysqlNativePassword(user.Auth1.Password)
			} else if len(user.Auth1.Plugin) > 0 {
				authUser.Auth1 = plan.NewOtherAuthentication(user.Auth1.Password, user.Auth1.Plugin)
			} else {
				// We default to using the password, even if it's empty
				authUser.Auth1 = plan.NewDefaultAuthentication(user.Auth1.Password)
			}
		}
		if user.Auth2 != nil || user.Auth3 != nil || user.AuthInitial != nil {
			return nil, fmt.Errorf(`multi-factor authentication is not yet supported`)
		}
		//TODO: figure out how to represent the remaining authentication methods and multi-factor auth
		authUsers[i] = authUser
	}
	var tlsOptions *plan.TLSOptions
	if n.TLSOptions != nil {
		tlsOptions = &plan.TLSOptions{
			SSL:     n.TLSOptions.SSL,
			X509:    n.TLSOptions.X509,
			Cipher:  n.TLSOptions.Cipher,
			Issuer:  n.TLSOptions.Issuer,
			Subject: n.TLSOptions.Subject,
		}
	}
	var accountLimits *plan.AccountLimits
	if n.AccountLimits != nil {
		var maxQueries *int64
		if n.AccountLimits.MaxQueriesPerHour != nil {
			if val, err := strconv.ParseInt(string(n.AccountLimits.MaxQueriesPerHour.Val), 10, 64); err != nil {
				return nil, err
			} else {
				maxQueries = &val
			}
		}
		var maxUpdates *int64
		if n.AccountLimits.MaxUpdatesPerHour != nil {
			if val, err := strconv.ParseInt(string(n.AccountLimits.MaxUpdatesPerHour.Val), 10, 64); err != nil {
				return nil, err
			} else {
				maxUpdates = &val
			}
		}
		var maxConnections *int64
		if n.AccountLimits.MaxConnectionsPerHour != nil {
			if val, err := strconv.ParseInt(string(n.AccountLimits.MaxConnectionsPerHour.Val), 10, 64); err != nil {
				return nil, err
			} else {
				maxConnections = &val
			}
		}
		var maxUserConnections *int64
		if n.AccountLimits.MaxUserConnections != nil {
			if val, err := strconv.ParseInt(string(n.AccountLimits.MaxUserConnections.Val), 10, 64); err != nil {
				return nil, err
			} else {
				maxUserConnections = &val
			}
		}
		accountLimits = &plan.AccountLimits{
			MaxQueriesPerHour:     maxQueries,
			MaxUpdatesPerHour:     maxUpdates,
			MaxConnectionsPerHour: maxConnections,
			MaxUserConnections:    maxUserConnections,
		}
	}
	var passwordOptions *plan.PasswordOptions
	if n.PasswordOptions != nil {
		var expirationTime *int64
		if n.PasswordOptions.ExpirationTime != nil {
			if val, err := strconv.ParseInt(string(n.PasswordOptions.ExpirationTime.Val), 10, 64); err != nil {
				return nil, err
			} else {
				expirationTime = &val
			}
		}
		var history *int64
		if n.PasswordOptions.History != nil {
			if val, err := strconv.ParseInt(string(n.PasswordOptions.History.Val), 10, 64); err != nil {
				return nil, err
			} else {
				history = &val
			}
		}
		var reuseInterval *int64
		if n.PasswordOptions.ReuseInterval != nil {
			if val, err := strconv.ParseInt(string(n.PasswordOptions.ReuseInterval.Val), 10, 64); err != nil {
				return nil, err
			} else {
				reuseInterval = &val
			}
		}
		var failedAttempts *int64
		if n.PasswordOptions.FailedAttempts != nil {
			if val, err := strconv.ParseInt(string(n.PasswordOptions.FailedAttempts.Val), 10, 64); err != nil {
				return nil, err
			} else {
				failedAttempts = &val
			}
		}
		var lockTime *int64
		if n.PasswordOptions.LockTime != nil {
			if val, err := strconv.ParseInt(string(n.PasswordOptions.LockTime.Val), 10, 64); err != nil {
				return nil, err
			} else {
				lockTime = &val
			}
		}
		passwordOptions = &plan.PasswordOptions{
			RequireCurrentOptional: n.PasswordOptions.RequireCurrentOptional,
			ExpirationTime:         expirationTime,
			History:                history,
			ReuseInterval:          reuseInterval,
			FailedAttempts:         failedAttempts,
			LockTime:               lockTime,
		}
	}
	return &plan.CreateUser{
		IfNotExists:     n.IfNotExists,
		Users:           authUsers,
		DefaultRoles:    convertAccountName(n.DefaultRoles...),
		TLSOptions:      tlsOptions,
		AccountLimits:   accountLimits,
		PasswordOptions: passwordOptions,
		Locked:          n.Locked,
		Attribute:       n.Attribute,
		MySQLDb:         sql.UnresolvedDatabase("mysql"),
	}, nil
}

func convertRenameUser(ctx *sql.Context, n *sqlparser.RenameUser) (*plan.RenameUser, error) {
	oldNames := make([]plan.UserName, len(n.Accounts))
	newNames := make([]plan.UserName, len(n.Accounts))
	for i, account := range n.Accounts {
		oldNames[i] = convertAccountName(account.From)[0]
		newNames[i] = convertAccountName(account.To)[0]
	}
	return plan.NewRenameUser(oldNames, newNames), nil
}

func convertGrantPrivilege(ctx *sql.Context, n *sqlparser.GrantPrivilege) (*plan.Grant, error) {
	var gau *plan.GrantUserAssumption
	if n.As != nil {
		gauType := plan.GrantUserAssumptionType_Default
		switch n.As.Type {
		case sqlparser.GrantUserAssumptionType_None:
			gauType = plan.GrantUserAssumptionType_None
		case sqlparser.GrantUserAssumptionType_All:
			gauType = plan.GrantUserAssumptionType_All
		case sqlparser.GrantUserAssumptionType_AllExcept:
			gauType = plan.GrantUserAssumptionType_AllExcept
		case sqlparser.GrantUserAssumptionType_Roles:
			gauType = plan.GrantUserAssumptionType_Roles
		}
		gau = &plan.GrantUserAssumption{
			Type:  gauType,
			User:  convertAccountName(n.As.User)[0],
			Roles: convertAccountName(n.As.Roles...),
		}
	}
	return plan.NewGrant(
		sql.UnresolvedDatabase("mysql"),
		convertPrivilege(n.Privileges...),
		convertObjectType(n.ObjectType),
		convertPrivilegeLevel(n.PrivilegeLevel),
		convertAccountName(n.To...),
		n.WithGrantOption,
		gau,
		ctx.Session.Client().User,
	)
}

func convertShowGrants(ctx *sql.Context, n *sqlparser.ShowGrants) (*plan.ShowGrants, error) {
	var currentUser bool
	var user *plan.UserName
	if n.For != nil {
		currentUser = false
		user = &convertAccountName(*n.For)[0]
	} else {
		currentUser = true
		client := ctx.Session.Client()
		user = &plan.UserName{
			Name:    client.User,
			Host:    client.Address,
			AnyHost: client.Address == "%",
		}
	}
	return plan.NewShowGrants(currentUser, user, convertAccountName(n.Using...)), nil
}

func convertFlush(ctx *sql.Context, f *sqlparser.Flush) (sql.Node, error) {
	var writesToBinlog = true
	switch strings.ToLower(f.Type) {
	case "no_write_to_binlog", "local":
		//writesToBinlog = false
		return nil, fmt.Errorf("%s not supported", f.Type)
	}

	switch strings.ToLower(f.Option.Name) {
	case "privileges":
		return plan.NewFlushPrivileges(writesToBinlog), nil
	default:
		return nil, fmt.Errorf("%s not supported", f.Option.Name)
	}
}

func columnsToStrings(cols sqlparser.Columns) []string {
	res := make([]string, len(cols))
	for i, c := range cols {
		res[i] = c.String()
	}

	return res
}

func insertRowsToNode(ctx *sql.Context, ir sqlparser.InsertRows) (sql.Node, error) {
	switch v := ir.(type) {
	case sqlparser.SelectStatement:
		return convertSelectStatement(ctx, v)
	case sqlparser.Values:
		return valuesToValues(ctx, v)
	default:
		return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(ir))
	}
}

func valuesToValues(ctx *sql.Context, v sqlparser.Values) (*plan.Values, error) {
	exprTuples := make([][]sql.Expression, len(v))
	for i, vt := range v {
		exprs := make([]sql.Expression, len(vt))
		exprTuples[i] = exprs
		for j, e := range vt {
			expr, err := ExprToExpression(ctx, e)
			if err != nil {
				return nil, err
			}

			exprs[j] = expr
		}
	}

	return plan.NewValues(exprTuples), nil
}

func tableExprsToTable(
	ctx *sql.Context,
	te sqlparser.TableExprs,
) (sql.Node, error) {
	if len(te) == 0 {
		return plan.NewResolvedDualTable(), nil
	}

	var nodes []sql.Node
	for _, t := range te {
		n, err := tableExprToTable(ctx, t)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, n)
	}

	if len(nodes) == 1 {
		return nodes[0], nil
	}

	join := plan.NewCrossJoin(nodes[0], nodes[1])
	for i := 2; i < len(nodes); i++ {
		join = plan.NewCrossJoin(join, nodes[i])
	}

	return join, nil
}

func tableExprToTable(
	ctx *sql.Context,
	te sqlparser.TableExpr,
) (sql.Node, error) {
	switch t := (te).(type) {
	default:
		return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(te))
	case *sqlparser.AliasedTableExpr:
		// TODO: Add support for qualifier.
		switch e := t.Expr.(type) {
		case sqlparser.TableName:
			var node *plan.UnresolvedTable
			if t.AsOf != nil {
				asOfExpr, err := ExprToExpression(ctx, t.AsOf.Time)
				if err != nil {
					return nil, err
				}
				node = plan.NewUnresolvedTableAsOf(e.Name.String(), e.Qualifier.String(), asOfExpr)
			} else {
				node = tableNameToUnresolvedTable(e)
			}

			if !t.As.IsEmpty() {
				return plan.NewTableAlias(t.As.String(), node), nil
			}

			return node, nil
		case *sqlparser.Subquery:
			node, err := convert(ctx, e.Select, sqlparser.String(e.Select))
			if err != nil {
				return nil, err
			}

			if t.As.IsEmpty() {
				// This should be caught by the parser, but here just in case
				return nil, sql.ErrUnsupportedFeature.New("subquery without alias")
			}

			sq := plan.NewSubqueryAlias(t.As.String(), sqlparser.String(e.Select), node)

			if len(e.Columns) > 0 {
				columns := columnsToStrings(e.Columns)
				sq = sq.WithColumns(columns)
			}

			return sq, nil
		case *sqlparser.ValuesStatement:
			if t.As.IsEmpty() {
				// Parser should enforce this, but just to be safe
				return nil, sql.ErrUnsupportedSyntax.New("every derived table must have an alias")
			}
			values, err := valuesToValues(ctx, e.Rows)
			if err != nil {
				return nil, err
			}

			vdt := plan.NewValueDerivedTable(values, t.As.String())

			if len(e.Columns) > 0 {
				columns := columnsToStrings(e.Columns)
				vdt = vdt.WithColumns(columns)
			}

			return vdt, nil
		default:
			return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(te))
		}

	case *sqlparser.TableFuncExpr:
		exprs, err := selectExprsToExpressions(ctx, t.Exprs)
		if err != nil {
			return nil, err
		}
		utf := expression.NewUnresolvedTableFunction(t.Name, exprs)
		if t.Alias.IsEmpty() {
			return plan.NewTableAlias(t.Name, utf), nil
		}
		return plan.NewTableAlias(t.Alias.String(), utf), nil

	case *sqlparser.JoinTableExpr:
		return joinTableExpr(ctx, t)

	case *sqlparser.JSONTableExpr:
		return jsonTableExpr(ctx, t)

	case *sqlparser.ParenTableExpr:
		if len(t.Exprs) == 1 {
			switch j := t.Exprs[0].(type) {
			case *sqlparser.JoinTableExpr:
				return joinTableExpr(ctx, j)
			default:
				return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(t))
			}
		} else {
			return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(t))
		}
	}
}

func joinTableExpr(ctx *sql.Context, t *sqlparser.JoinTableExpr) (sql.Node, error) {
	// TODO: add support for using, once we have proper table
	// qualification of fields
	if len(t.Condition.Using) > 0 {
		return nil, sql.ErrUnsupportedFeature.New("USING clause on join")
	}

	left, err := tableExprToTable(ctx, t.LeftExpr)
	if err != nil {
		return nil, err
	}

	right, err := tableExprToTable(ctx, t.RightExpr)
	if err != nil {
		return nil, err
	}

	if t.Join == sqlparser.NaturalJoinStr {
		return plan.NewNaturalJoin(left, right), nil
	}

	if t.Condition.On == nil {
		return plan.NewCrossJoin(left, right), nil
	}

	cond, err := ExprToExpression(ctx, t.Condition.On)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(t.Join) {
	case sqlparser.JoinStr:
		return plan.NewInnerJoin(left, right, cond), nil
	case sqlparser.LeftJoinStr:
		return plan.NewLeftOuterJoin(left, right, cond), nil
	case sqlparser.RightJoinStr:
		return plan.NewRightOuterJoin(left, right, cond), nil
	case sqlparser.FullOuterJoinStr:
		return plan.NewFullOuterJoin(left, right, cond), nil
	default:
		return nil, sql.ErrUnsupportedFeature.New("Join type " + t.Join)
	}
}

func jsonTableCols(ctx *sql.Context, jtSpec *sqlparser.JSONTableSpec) ([]plan.JSONTableCol, error) {
	var cols []plan.JSONTableCol
	for _, jtColDef := range jtSpec.Columns {
		if jtColDef.Spec != nil {
			nestedCols, err := jsonTableCols(ctx, jtColDef.Spec)
			if err != nil {
				return nil, err
			}
			col := plan.JSONTableCol{
				Path:       jtColDef.Spec.Path,
				NestedCols: nestedCols,
			}
			cols = append(cols, col)
			continue
		}

		typ, err := types.ColumnTypeToType(&jtColDef.Type)
		if err != nil {
			return nil, err
		}

		var defEmptyVal, defErrorVal sql.Expression
		if jtColDef.Opts.ValOnEmpty == nil {
			defEmptyVal = expression.NewLiteral(nil, types.Null)
		} else {
			defEmptyVal, err = ExprToExpression(ctx, jtColDef.Opts.ValOnEmpty)
			if err != nil {
				return nil, err
			}
		}

		if jtColDef.Opts.ValOnError == nil {
			defErrorVal = expression.NewLiteral(nil, types.Null)
		} else {
			defErrorVal, err = ExprToExpression(ctx, jtColDef.Opts.ValOnError)
			if err != nil {
				return nil, err
			}
		}

		planCol := plan.JSONTableCol{
			Path: jtColDef.Opts.Path,
			Opts: &plan.JSONTableColOpts{
				Name:         jtColDef.Name.String(),
				Type:         typ,
				ForOrd:       bool(jtColDef.Type.Autoincrement),
				Exists:       jtColDef.Opts.Exists,
				DefEmptyVal:  defEmptyVal,
				DefErrorVal:  defErrorVal,
				ErrorOnEmpty: jtColDef.Opts.ErrorOnEmpty,
				ErrorOnError: jtColDef.Opts.ErrorOnError,
			},
		}
		cols = append(cols, planCol)
	}
	return cols, nil
}

func jsonTableExpr(ctx *sql.Context, t *sqlparser.JSONTableExpr) (sql.Node, error) {
	data, err := ExprToExpression(ctx, t.Data)
	if err != nil {
		return nil, err
	}
	alias := t.Alias.String()
	cols, err := jsonTableCols(ctx, t.Spec)
	if err != nil {
		return nil, err
	}
	return plan.NewJSONTable(data, t.Spec.Path, alias, cols)
}

func whereToFilter(ctx *sql.Context, w *sqlparser.Where, child sql.Node) (*plan.Filter, error) {
	c, err := ExprToExpression(ctx, w.Expr)
	if err != nil {
		return nil, err
	}

	return plan.NewFilter(c, child), nil
}

func orderByToSort(ctx *sql.Context, ob sqlparser.OrderBy, child sql.Node) (*plan.Sort, error) {
	sortFields, err := orderByToSortFields(ctx, ob)
	if err != nil {
		return nil, err
	}

	return plan.NewSort(sortFields, child), nil
}

func orderByToSortFields(ctx *sql.Context, ob sqlparser.OrderBy) (sql.SortFields, error) {
	var sortFields sql.SortFields
	for _, o := range ob {
		e, err := ExprToExpression(ctx, o.Expr)
		if err != nil {
			return nil, err
		}

		var so sql.SortOrder
		switch strings.ToLower(o.Direction) {
		default:
			return nil, errInvalidSortOrder.New(o.Direction)
		case sqlparser.AscScr:
			so = sql.Ascending
		case sqlparser.DescScr:
			so = sql.Descending
		}

		e2, _ := e.(sql.Expression2)
		sf := sql.SortField{
			Column:  e,
			Column2: e2,
			Order:   so,
		}
		sortFields = append(sortFields, sf)
	}
	return sortFields, nil
}

func limitToLimit(
	ctx *sql.Context,
	limit sqlparser.Expr,
	child sql.Node,
) (*plan.Limit, error) {
	rowCount, err := ExprToExpression(ctx, limit)
	if err != nil {
		return nil, err
	}

	return plan.NewLimit(rowCount, child), nil
}

func havingToHaving(ctx *sql.Context, having *sqlparser.Where, node sql.Node) (sql.Node, error) {
	cond, err := ExprToExpression(ctx, having.Expr)
	if err != nil {
		return nil, err
	}

	return plan.NewHaving(cond, node), nil
}

// windowToWindow wraps a plan.Window node in a plan.NamedWindows node.
func windowToWindow(ctx *sql.Context, windowDefs sqlparser.Window, window *plan.Window) (sql.Node, error) {
	newWindowDefs := make(map[string]*sql.WindowDefinition, len(windowDefs))
	var err error
	for _, def := range windowDefs {
		newWindowDefs[def.Name.Lowered()], err = windowDefToWindow(ctx, def)
		if err != nil {
			return nil, err
		}
	}
	return plan.NewNamedWindows(newWindowDefs, window), nil
}

func offsetToOffset(
	ctx *sql.Context,
	offset sqlparser.Expr,
	child sql.Node,
) (sql.Node, error) {
	rowCount, err := ExprToExpression(ctx, offset)
	if err != nil {
		return nil, err
	}

	// Check if offset starts at 0, if so, we can just remove the offset node.
	// Only cast to int8, as a larger int type just means a non-zero offset.
	if val, err := rowCount.Eval(ctx, nil); err == nil {
		if v, ok := val.(int8); ok && v == 0 {
			return child, nil
		}
	}

	return plan.NewOffset(rowCount, child), nil
}

// getInt64Literal returns an int64 *expression.Literal for the value given, or an unsupported error with the string
// given if the expression doesn't represent an integer literal.
func getInt64Literal(ctx *sql.Context, expr sqlparser.Expr, errStr string) (*expression.Literal, error) {
	e, err := ExprToExpression(ctx, expr)
	if err != nil {
		return nil, err
	}

	switch e := e.(type) {
	case *expression.Literal:
		if !types.IsInteger(e.Type()) {
			return nil, sql.ErrUnsupportedFeature.New(errStr)
		}
	}
	nl, ok := e.(*expression.Literal)
	if !ok || !types.IsInteger(nl.Type()) {
		return nil, sql.ErrUnsupportedFeature.New(errStr)
	} else {
		i64, _, err := types.Int64.Convert(nl.Value())
		if err != nil {
			return nil, sql.ErrUnsupportedFeature.New(errStr)
		}
		return expression.NewLiteral(i64, types.Int64), nil
	}

	return nl, nil
}

// getInt64Value returns the int64 literal value in the expression given, or an error with the errStr given if it
// cannot.
func getInt64Value(ctx *sql.Context, expr sqlparser.Expr, errStr string) (int64, error) {
	ie, err := getInt64Literal(ctx, expr, errStr)
	if err != nil {
		return 0, err
	}

	i, err := ie.Eval(ctx, nil)
	if err != nil {
		return 0, err
	}

	return i.(int64), nil
}

func isAggregateExpr(e sql.Expression) bool {
	var isAgg bool
	sql.Inspect(e, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.UnresolvedFunction:
			isAgg = isAgg || e.IsAggregate
		case *aggregation.CountDistinct, *aggregation.GroupConcat:
			isAgg = true
		}

		return true
	})
	return isAgg
}

func selectToSelectionNode(
	ctx *sql.Context,
	se sqlparser.SelectExprs,
	g sqlparser.GroupBy,
	child sql.Node,
) (sql.Node, error) {
	selectExprs, err := selectExprsToExpressions(ctx, se)
	if err != nil {
		return nil, err
	}

	isWindow := false
	for _, e := range selectExprs {
		if isWindowExpr(e) {
			isWindow = true
			break
		}
	}

	if isWindow {
		if len(g) > 0 {
			return nil, sql.ErrUnsupportedFeature.New("group by with window functions")
		}
		return plan.NewWindow(selectExprs, child), nil
	}

	isAgg := len(g) > 0
	if !isAgg {
		for _, e := range selectExprs {
			if isAggregateExpr(e) {
				isAgg = true
				break
			}
		}
	}

	if isAgg {
		groupingExprs, err := groupByToExpressions(ctx, g)
		if err != nil {
			return nil, err
		}

		agglen := int64(len(selectExprs))
		for i, ge := range groupingExprs {
			// if GROUP BY index
			if l, ok := ge.(*expression.Literal); ok && types.IsNumber(l.Type()) {
				if i64, _, err := types.Int64.Convert(l.Value()); err == nil {
					if idx, ok := i64.(int64); ok && idx > 0 && idx <= agglen {
						aggexpr := selectExprs[idx-1]
						if alias, ok := aggexpr.(*expression.Alias); ok {
							aggexpr = expression.NewUnresolvedColumn(alias.Name())
						}
						groupingExprs[i] = aggexpr
					}
				}
			}
		}

		return plan.NewGroupBy(selectExprs, groupingExprs, child), nil
	}

	return plan.NewProject(selectExprs, child), nil
}

func isWindowExpr(e sql.Expression) bool {
	isWindow := false
	sql.Inspect(e, func(e sql.Expression) bool {
		if uf, ok := e.(*expression.UnresolvedFunction); ok {
			if uf.Window != nil {
				isWindow = true
				return false
			}
		}
		return true
	})

	return isWindow
}

func selectExprsToExpressions(ctx *sql.Context, se sqlparser.SelectExprs) ([]sql.Expression, error) {
	var exprs []sql.Expression
	for _, e := range se {
		pe, err := selectExprToExpression(ctx, e)
		if err != nil {
			return nil, err
		}

		exprs = append(exprs, pe)
	}

	return exprs, nil
}

// StringToColumnDefaultValue takes in a string representing a default value and returns the equivalent Expression.
func StringToColumnDefaultValue(ctx *sql.Context, exprStr string) (*sql.ColumnDefaultValue, error) {
	// all valid default expressions will parse correctly with SELECT prepended, as the parser will not parse raw expressions
	stmt, err := sqlparser.Parse("SELECT " + exprStr)
	if err != nil {
		return nil, err
	}
	parserSelect, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("DefaultStringToExpression expected sqlparser.Select but received %T", stmt)
	}
	if len(parserSelect.SelectExprs) != 1 {
		return nil, fmt.Errorf("default string does not have only one expression")
	}
	aliasedExpr, ok := parserSelect.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("DefaultStringToExpression expected *sqlparser.AliasedExpr but received %T", parserSelect.SelectExprs[0])
	}
	parsedExpr, err := ExprToExpression(ctx, aliasedExpr.Expr)
	if err != nil {
		return nil, err
	}
	// The literal and function expression distinction is based primarily on the presence of parentheses,
	// with the exception that now/current_timestamp are not literal expressions but can be used without
	// parens
	_, isParenthesized := aliasedExpr.Expr.(*sqlparser.ParenExpr)

	var isLiteral bool
	switch e := parsedExpr.(type) {
	case *expression.UnaryMinus:
		_, isLiteral = e.Child.(*expression.Literal)
	case *expression.UnresolvedFunction:
		isLiteral = false
	default:
		isLiteral = len(parsedExpr.Children()) == 0 && !strings.HasPrefix(exprStr, "(")
	}
	return ExpressionToColumnDefaultValue(ctx, parsedExpr, isLiteral, isParenthesized)
}

// ExpressionToColumnDefaultValue takes in an Expression and returns the equivalent ColumnDefaultValue if the expression
// is valid for a default value. If the expression represents a literal (and not an expression that returns a literal, so "5"
// rather than "(5)"), then the parameter "isLiteral" should be true.
func ExpressionToColumnDefaultValue(_ *sql.Context, inputExpr sql.Expression, isLiteral, isParenthesized bool) (*sql.ColumnDefaultValue, error) {
	return sql.NewColumnDefaultValue(inputExpr, nil, isLiteral, isParenthesized, true)
}

// MustStringToColumnDefaultValue is used for creating default values on tables that do not go through the analyzer. Does not handle
// function nor column references.
func MustStringToColumnDefaultValue(ctx *sql.Context, exprStr string, outType sql.Type, nullable bool) *sql.ColumnDefaultValue {
	expr, err := StringToColumnDefaultValue(ctx, exprStr)
	if err != nil {
		panic(err)
	}
	expr, err = sql.NewColumnDefaultValue(expr.Expression, outType, expr.IsLiteral(), !expr.IsLiteral(), nullable)
	if err != nil {
		panic(err)
	}
	return expr
}

func ExprToExpression(ctx *sql.Context, e sqlparser.Expr) (sql.Expression, error) {
	switch v := e.(type) {
	default:
		return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(e))
	case *sqlparser.Default:
		return expression.NewDefaultColumn(v.ColName), nil
	case *sqlparser.SubstrExpr:
		var (
			name sql.Expression
			err  error
		)
		if v.Name != nil {
			name, err = ExprToExpression(ctx, v.Name)
		} else {
			name, err = ExprToExpression(ctx, v.StrVal)
		}
		if err != nil {
			return nil, err
		}
		from, err := ExprToExpression(ctx, v.From)
		if err != nil {
			return nil, err
		}

		if v.To == nil {
			return function.NewSubstring(name, from)
		}
		to, err := ExprToExpression(ctx, v.To)
		if err != nil {
			return nil, err
		}
		return function.NewSubstring(name, from, to)
	case *sqlparser.CurTimeFuncExpr:
		fsp, err := ExprToExpression(ctx, v.Fsp)
		if err != nil {
			return nil, err
		}
		return function.NewCurrTimestamp(fsp)
	case *sqlparser.TrimExpr:
		var (
			pat sql.Expression
			str sql.Expression
			err error
		)
		pat, err = ExprToExpression(ctx, v.Pattern)
		str, err = ExprToExpression(ctx, v.Str)
		return function.NewTrim(str, pat, v.Dir), err
	case *sqlparser.ComparisonExpr:
		return comparisonExprToExpression(ctx, v)
	case *sqlparser.IsExpr:
		return isExprToExpression(ctx, v)
	case *sqlparser.NotExpr:
		c, err := ExprToExpression(ctx, v.Expr)
		if err != nil {
			return nil, err
		}

		return expression.NewNot(c), nil
	case *sqlparser.SQLVal:
		return convertVal(ctx, v)
	case sqlparser.BoolVal:
		return expression.NewLiteral(bool(v), types.Boolean), nil
	case *sqlparser.NullVal:
		return expression.NewLiteral(nil, types.Null), nil
	case *sqlparser.ColName:
		if !v.Qualifier.IsEmpty() {
			return expression.NewUnresolvedQualifiedColumn(
				v.Qualifier.Name.String(),
				v.Name.String(),
			), nil
		}
		return expression.NewUnresolvedColumn(v.Name.String()), nil
	case *sqlparser.FuncExpr:
		exprs, err := selectExprsToExpressions(ctx, v.Exprs)
		if err != nil {
			return nil, err
		}

		// NOTE: The count distinct expressions work differently due to the * syntax. eg. COUNT(*)
		if v.Distinct && v.Name.Lowered() == "count" {
			return aggregation.NewCountDistinct(exprs...), nil
		}

		// NOTE: Not all aggregate functions support DISTINCT. Fortunately, the vitess parser will throw
		// errors for when DISTINCT is used on aggregate functions that don't support DISTINCT.
		if v.Distinct {
			if len(exprs) != 1 {
				return nil, sql.ErrUnsupportedSyntax.New("more than one expression with distinct")
			}

			exprs[0] = expression.NewDistinctExpression(exprs[0])
		}
		over, err := windowDefToWindow(ctx, (*sqlparser.WindowDef)(v.Over))
		if err != nil {
			return nil, err
		}
		return expression.NewUnresolvedFunction(v.Name.Lowered(),
			isAggregateFunc(v), over, exprs...), nil
	case *sqlparser.GroupConcatExpr:
		exprs, err := selectExprsToExpressions(ctx, v.Exprs)
		if err != nil {
			return nil, err
		}

		separatorS := ","
		if !v.Separator.DefaultSeparator {
			separatorS = v.Separator.SeparatorString
		}

		sortFields, err := orderByToSortFields(ctx, v.OrderBy)
		if err != nil {
			return nil, err
		}

		//TODO: this should be acquired at runtime, not at parse time, so fix this
		gcml, err := ctx.GetSessionVariable(ctx, "group_concat_max_len")
		if err != nil {
			return nil, err
		}
		groupConcatMaxLen := gcml.(uint64)

		return aggregation.NewGroupConcat(v.Distinct, sortFields, separatorS, exprs, int(groupConcatMaxLen)), nil
	case *sqlparser.ParenExpr:
		return ExprToExpression(ctx, v.Expr)
	case *sqlparser.AndExpr:
		lhs, err := ExprToExpression(ctx, v.Left)
		if err != nil {
			return nil, err
		}

		rhs, err := ExprToExpression(ctx, v.Right)
		if err != nil {
			return nil, err
		}

		return expression.NewAnd(lhs, rhs), nil
	case *sqlparser.OrExpr:
		lhs, err := ExprToExpression(ctx, v.Left)
		if err != nil {
			return nil, err
		}

		rhs, err := ExprToExpression(ctx, v.Right)
		if err != nil {
			return nil, err
		}

		return expression.NewOr(lhs, rhs), nil
	case *sqlparser.XorExpr:
		lhs, err := ExprToExpression(ctx, v.Left)
		if err != nil {
			return nil, err
		}

		rhs, err := ExprToExpression(ctx, v.Right)
		if err != nil {
			return nil, err
		}

		return expression.NewXor(lhs, rhs), nil
	case *sqlparser.ConvertUsingExpr:
		expr, err := ExprToExpression(ctx, v.Expr)
		if err != nil {
			return nil, err
		}

		collation, err := sql.ParseCollation(&v.Type, nil, false)
		if err != nil {
			return nil, err
		}

		return expression.NewCollatedExpression(expr, collation), nil
	case *sqlparser.ConvertExpr:
		expr, err := ExprToExpression(ctx, v.Expr)
		if err != nil {
			return nil, err
		}

		typeLength := 0
		if v.Type.Length != nil {
			typeLength, err = strconv.Atoi(v.Type.Length.String())
			if err != nil {
				return nil, err
			}
		}

		typeScale := 0
		if v.Type.Scale != nil {
			typeScale, err = strconv.Atoi(v.Type.Scale.String())
			if err != nil {
				return nil, err
			}
		}

		return expression.NewConvertWithLengthAndScale(expr, v.Type.Type, typeLength, typeScale), nil
	case *sqlparser.RangeCond:
		val, err := ExprToExpression(ctx, v.Left)
		if err != nil {
			return nil, err
		}

		lower, err := ExprToExpression(ctx, v.From)
		if err != nil {
			return nil, err
		}

		upper, err := ExprToExpression(ctx, v.To)
		if err != nil {
			return nil, err
		}

		switch strings.ToLower(v.Operator) {
		case sqlparser.BetweenStr:
			return expression.NewBetween(val, lower, upper), nil
		case sqlparser.NotBetweenStr:
			return expression.NewNot(expression.NewBetween(val, lower, upper)), nil
		default:
			return nil, sql.ErrUnsupportedFeature.New(fmt.Sprintf("RangeCond with operator: %s", v.Operator))
		}
	case sqlparser.ValTuple:
		var exprs = make([]sql.Expression, len(v))
		for i, e := range v {
			expr, err := ExprToExpression(ctx, e)
			if err != nil {
				return nil, err
			}
			exprs[i] = expr
		}
		return expression.NewTuple(exprs...), nil

	case *sqlparser.BinaryExpr:
		return binaryExprToExpression(ctx, v)
	case *sqlparser.UnaryExpr:
		return unaryExprToExpression(ctx, v)
	case *sqlparser.Subquery:
		node, err := convert(ctx, v.Select, "")
		if err != nil {
			return nil, err
		}

		// TODO: get the original select statement, not the reconstruction
		selectString := sqlparser.String(v.Select)
		return plan.NewSubquery(node, selectString), nil
	case *sqlparser.CaseExpr:
		return caseExprToExpression(ctx, v)
	case *sqlparser.IntervalExpr:
		return intervalExprToExpression(ctx, v)
	case *sqlparser.CollateExpr:
		return handleCollateExpr(ctx, ctx.GetCharacterSet(), v)
	case *sqlparser.ValuesFuncExpr:
		col, err := ExprToExpression(ctx, v.Name)
		if err != nil {
			return nil, err
		}
		return expression.NewUnresolvedFunction("values", false, nil, col), nil
	case *sqlparser.ExistsExpr:
		subqueryExp, err := ExprToExpression(ctx, v.Subquery)
		if err != nil {
			return nil, err
		}
		sq, ok := subqueryExp.(*plan.Subquery)
		if !ok {
			return nil, fmt.Errorf("expected subquery expression, found: %T", subqueryExp)
		}
		return plan.NewExistsSubquery(sq), nil
	case *sqlparser.TimestampFuncExpr:
		var (
			unit  sql.Expression
			expr1 sql.Expression
			expr2 sql.Expression
			err   error
		)

		unit = expression.NewLiteral(v.Unit, types.LongText)
		expr1, err = ExprToExpression(ctx, v.Expr1)
		if err != nil {
			return nil, err
		}
		expr2, err = ExprToExpression(ctx, v.Expr2)
		if err != nil {
			return nil, err
		}

		if v.Name == "timestampdiff" {
			return function.NewTimestampDiff(unit, expr1, expr2), nil
		} else if v.Name == "timestampadd" {
			return nil, fmt.Errorf("TIMESTAMPADD() not supported")
		}
		return nil, nil
	case *sqlparser.ExtractFuncExpr:
		var unit sql.Expression = expression.NewLiteral(strings.ToUpper(v.Unit), types.LongText)
		expr, err := ExprToExpression(ctx, v.Expr)
		if err != nil {
			return nil, err
		}
		return function.NewExtract(unit, expr), err
	}
}

// handleCollateExpr is meant to handle generic text-returning expressions that should be reinterpreted as a different collation.
func handleCollateExpr(ctx *sql.Context, charSet sql.CharacterSetID, expr *sqlparser.CollateExpr) (sql.Expression, error) {
	innerExpr, err := ExprToExpression(ctx, expr.Expr)
	if err != nil {
		return nil, err
	}
	//TODO: rename this from Charset to Collation
	collation, err := sql.ParseCollation(nil, &expr.Charset, false)
	if err != nil {
		return nil, err
	}
	// If we're collating a string literal, we check that the charset and collation match now. Other string sources
	// (such as from tables) will have their own charset, which we won't know until after the parsing stage.
	if _, isLiteral := innerExpr.(*expression.Literal); isLiteral && collation.CharacterSet() != charSet {
		return nil, sql.ErrCollationInvalidForCharSet.New(collation.Name(), charSet.Name())
	}
	return expression.NewCollatedExpression(innerExpr, collation), nil
}

func windowDefToWindow(ctx *sql.Context, def *sqlparser.WindowDef) (*sql.WindowDefinition, error) {
	if def == nil {
		return nil, nil
	}

	sortFields, err := orderByToSortFields(ctx, def.OrderBy)
	if err != nil {
		return nil, err
	}

	partitions := make([]sql.Expression, len(def.PartitionBy))
	for i, expr := range def.PartitionBy {
		var err error
		partitions[i], err = ExprToExpression(ctx, expr)
		if err != nil {
			return nil, err
		}
	}

	frame, err := NewFrame(ctx, def.Frame)
	if err != nil {
		return nil, err
	}

	// According to MySQL documentation at https://dev.mysql.com/doc/refman/8.0/en/window-functions-usage.html
	// "If OVER() is empty, the window consists of all query rows and the window function computes a result using all rows."
	if def.OrderBy == nil && frame == nil {
		frame = plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame()
	}

	return sql.NewWindowDefinition(partitions, sortFields, frame, def.NameRef.Lowered(), def.Name.Lowered()), nil
}

func isAggregateFunc(v *sqlparser.FuncExpr) bool {
	switch v.Name.Lowered() {
	case "first", "last", "count", "sum", "any_value", "avg", "max", "min",
		"count_distinct", "json_arrayagg",
		"row_number", "percent_rank", "lag", "first_value":
		return true
	}

	return v.IsAggregate()
}

// Convert an integer, represented by the specified string in the specified
// base, to its smallest representation possible, out of:
// int8, uint8, int16, uint16, int32, uint32, int64 and uint64
func convertInt(value string, base int) (sql.Expression, error) {
	if i8, err := strconv.ParseInt(value, base, 8); err == nil {
		return expression.NewLiteral(int8(i8), types.Int8), nil
	}
	if ui8, err := strconv.ParseUint(value, base, 8); err == nil {
		return expression.NewLiteral(uint8(ui8), types.Uint8), nil
	}
	if i16, err := strconv.ParseInt(value, base, 16); err == nil {
		return expression.NewLiteral(int16(i16), types.Int16), nil
	}
	if ui16, err := strconv.ParseUint(value, base, 16); err == nil {
		return expression.NewLiteral(uint16(ui16), types.Uint16), nil
	}
	if i32, err := strconv.ParseInt(value, base, 32); err == nil {
		return expression.NewLiteral(int32(i32), types.Int32), nil
	}
	if ui32, err := strconv.ParseUint(value, base, 32); err == nil {
		return expression.NewLiteral(uint32(ui32), types.Uint32), nil
	}
	if i64, err := strconv.ParseInt(value, base, 64); err == nil {
		return expression.NewLiteral(int64(i64), types.Int64), nil
	}
	if ui64, err := strconv.ParseUint(value, base, 64); err == nil {
		return expression.NewLiteral(uint64(ui64), types.Uint64), nil
	}
	if decimal, _, err := types.InternalDecimalType.Convert(value); err == nil {
		return expression.NewLiteral(decimal, types.InternalDecimalType), nil
	}

	return nil, fmt.Errorf("could not convert %s to any numerical type", value)
}

func convertVal(ctx *sql.Context, v *sqlparser.SQLVal) (sql.Expression, error) {
	switch v.Type {
	case sqlparser.StrVal:
		return expression.NewLiteral(string(v.Val), types.CreateLongText(ctx.GetCollation())), nil
	case sqlparser.IntVal:
		return convertInt(string(v.Val), 10)
	case sqlparser.FloatVal:
		val, err := strconv.ParseFloat(string(v.Val), 64)
		if err != nil {
			return nil, err
		}

		// use the value as string format to keep precision and scale as defined for DECIMAL data type to avoid rounded up float64 value
		if ps := strings.Split(string(v.Val), "."); len(ps) == 2 {
			ogVal := string(v.Val)
			var fmtStr byte = 'f'
			if strings.Contains(ogVal, "e") {
				fmtStr = 'e'
			}
			floatVal := strconv.FormatFloat(val, fmtStr, -1, 64)
			if len(ogVal) >= len(floatVal) && ogVal != floatVal {
				p, s := expression.GetDecimalPrecisionAndScale(ogVal)
				dt, err := types.CreateDecimalType(p, s)
				if err != nil {
					return expression.NewLiteral(string(v.Val), types.CreateLongText(ctx.GetCollation())), nil
				}
				dVal, _, err := dt.Convert(ogVal)
				if err != nil {
					return expression.NewLiteral(string(v.Val), types.CreateLongText(ctx.GetCollation())), nil
				}
				return expression.NewLiteral(dVal, dt), nil
			}
		}

		return expression.NewLiteral(val, types.Float64), nil
	case sqlparser.HexNum:
		//TODO: binary collation?
		v := strings.ToLower(string(v.Val))
		if strings.HasPrefix(v, "0x") {
			v = v[2:]
		} else if strings.HasPrefix(v, "x") {
			v = strings.Trim(v[1:], "'")
		}

		valBytes := []byte(v)
		dst := make([]byte, hex.DecodedLen(len(valBytes)))
		_, err := hex.Decode(dst, valBytes)
		if err != nil {
			return nil, err
		}
		return expression.NewLiteral(dst, types.LongBlob), nil
	case sqlparser.HexVal:
		//TODO: binary collation?
		val, err := v.HexDecode()
		if err != nil {
			return nil, err
		}
		return expression.NewLiteral(val, types.LongBlob), nil
	case sqlparser.ValArg:
		return expression.NewBindVar(strings.TrimPrefix(string(v.Val), ":")), nil
	case sqlparser.BitVal:
		if len(v.Val) == 0 {
			return expression.NewLiteral(0, types.Uint64), nil
		}

		res, err := strconv.ParseUint(string(v.Val), 2, 64)
		if err != nil {
			return nil, err
		}

		return expression.NewLiteral(res, types.Uint64), nil
	}

	return nil, sql.ErrInvalidSQLValType.New(v.Type)
}

func isExprToExpression(ctx *sql.Context, c *sqlparser.IsExpr) (sql.Expression, error) {
	e, err := ExprToExpression(ctx, c.Expr)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(c.Operator) {
	case sqlparser.IsNullStr:
		return expression.NewIsNull(e), nil
	case sqlparser.IsNotNullStr:
		return expression.NewNot(expression.NewIsNull(e)), nil
	case sqlparser.IsTrueStr:
		return expression.NewIsTrue(e), nil
	case sqlparser.IsFalseStr:
		return expression.NewIsFalse(e), nil
	case sqlparser.IsNotTrueStr:
		return expression.NewNot(expression.NewIsTrue(e)), nil
	case sqlparser.IsNotFalseStr:
		return expression.NewNot(expression.NewIsFalse(e)), nil
	default:
		return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(c))
	}
}

func comparisonExprToExpression(ctx *sql.Context, c *sqlparser.ComparisonExpr) (sql.Expression, error) {
	left, err := ExprToExpression(ctx, c.Left)
	if err != nil {
		return nil, err
	}

	right, err := ExprToExpression(ctx, c.Right)
	if err != nil {
		return nil, err
	}

	var escape sql.Expression = nil
	if c.Escape != nil {
		escape, err = ExprToExpression(ctx, c.Escape)
		if err != nil {
			return nil, err
		}
	}

	switch strings.ToLower(c.Operator) {
	case sqlparser.RegexpStr:
		return expression.NewRegexp(left, right), nil
	case sqlparser.NotRegexpStr:
		return expression.NewNot(expression.NewRegexp(left, right)), nil
	case sqlparser.EqualStr:
		return expression.NewEquals(left, right), nil
	case sqlparser.LessThanStr:
		return expression.NewLessThan(left, right), nil
	case sqlparser.LessEqualStr:
		return expression.NewLessThanOrEqual(left, right), nil
	case sqlparser.GreaterThanStr:
		return expression.NewGreaterThan(left, right), nil
	case sqlparser.GreaterEqualStr:
		return expression.NewGreaterThanOrEqual(left, right), nil
	case sqlparser.NullSafeEqualStr:
		return expression.NewNullSafeEquals(left, right), nil
	case sqlparser.NotEqualStr:
		return expression.NewNot(
			expression.NewEquals(left, right),
		), nil
	case sqlparser.InStr:
		switch right.(type) {
		case expression.Tuple:
			return expression.NewInTuple(left, right), nil
		case *plan.Subquery:
			return plan.NewInSubquery(left, right), nil
		default:
			return nil, sql.ErrUnsupportedFeature.New(fmt.Sprintf("IN %T", right))
		}
	case sqlparser.NotInStr:
		switch right.(type) {
		case expression.Tuple:
			return expression.NewNotInTuple(left, right), nil
		case *plan.Subquery:
			return plan.NewNotInSubquery(left, right), nil
		default:
			return nil, sql.ErrUnsupportedFeature.New(fmt.Sprintf("NOT IN %T", right))
		}
	case sqlparser.LikeStr:
		return expression.NewLike(left, right, escape), nil
	case sqlparser.NotLikeStr:
		return expression.NewNot(expression.NewLike(left, right, escape)), nil
	default:
		return nil, sql.ErrUnsupportedFeature.New(c.Operator)
	}
}

func groupByToExpressions(ctx *sql.Context, g sqlparser.GroupBy) ([]sql.Expression, error) {
	es := make([]sql.Expression, len(g))
	for i, ve := range g {
		e, err := ExprToExpression(ctx, ve)
		if err != nil {
			return nil, err
		}

		es[i] = e
	}

	return es, nil
}

func selectExprToExpression(ctx *sql.Context, se sqlparser.SelectExpr) (sql.Expression, error) {
	switch e := se.(type) {
	default:
		return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(e))
	case *sqlparser.StarExpr:
		if e.TableName.IsEmpty() {
			return expression.NewStar(), nil
		}
		return expression.NewQualifiedStar(e.TableName.Name.String()), nil
	case *sqlparser.AliasedExpr:
		expr, err := ExprToExpression(ctx, e.Expr)
		if err != nil {
			return nil, err
		}

		if !e.As.IsEmpty() {
			return expression.NewAlias(e.As.String(), expr), nil
		}

		if selectExprNeedsAlias(e, expr) {
			return expression.NewAlias(e.InputExpression, expr), nil
		}

		return expr, nil
	}
}

func selectExprNeedsAlias(e *sqlparser.AliasedExpr, expr sql.Expression) bool {
	if len(e.InputExpression) == 0 {
		return false
	}

	// We want to avoid unnecessary wrapping of aliases, but not at the cost of blowing up parse time. So we examine
	// the expression tree to see if is likely to need an alias without first serializing the expression being
	// examined, which can be very expensive in memory.
	complex := false
	sql.Inspect(expr, func(expr sql.Expression) bool {
		switch expr.(type) {
		case *plan.Subquery, *expression.UnresolvedFunction, *expression.Case, *expression.InTuple, *plan.InSubquery, *expression.HashInTuple:
			complex = true
			return false
		default:
			return true
		}
	})

	return complex || e.InputExpression != expr.String()
}

func unaryExprToExpression(ctx *sql.Context, e *sqlparser.UnaryExpr) (sql.Expression, error) {
	switch strings.ToLower(e.Operator) {
	case sqlparser.MinusStr:
		expr, err := ExprToExpression(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
		return expression.NewUnaryMinus(expr), nil
	case sqlparser.PlusStr:
		// Unary plus expressions do nothing (do not turn the expression positive). Just return the underlying expression.
		return ExprToExpression(ctx, e.Expr)
	case sqlparser.BinaryStr:
		expr, err := ExprToExpression(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
		return expression.NewBinary(expr), nil
	case sqlparser.BangStr:
		c, err := ExprToExpression(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
		return expression.NewNot(c), nil
	default:
		lowerOperator := strings.TrimSpace(strings.ToLower(e.Operator))
		if strings.HasPrefix(lowerOperator, "_") {
			// This is a character set introducer, so we need to decode the string to our internal encoding (`utf8mb4`)
			charSet, err := sql.ParseCharacterSet(lowerOperator[1:])
			if err != nil {
				return nil, err
			}
			if charSet.Encoder() == nil {
				return nil, sql.ErrUnsupportedFeature.New("unsupported character set: " + charSet.Name())
			}

			// Due to how vitess orders expressions, COLLATE is a child rather than a parent, so we need to handle it in a special way
			collation := charSet.DefaultCollation()
			if collateExpr, ok := e.Expr.(*sqlparser.CollateExpr); ok {
				// We extract the expression out of CollateExpr as we're only concerned about the collation string
				e.Expr = collateExpr.Expr
				// TODO: rename this from Charset to Collation
				collation, err = sql.ParseCollation(nil, &collateExpr.Charset, false)
				if err != nil {
					return nil, err
				}
				if collation.CharacterSet() != charSet {
					return nil, sql.ErrCollationInvalidForCharSet.New(collation.Name(), charSet.Name())
				}
			}

			// Character set introducers only work on string literals
			expr, err := ExprToExpression(ctx, e.Expr)
			if err != nil {
				return nil, err
			}
			if _, ok := expr.(*expression.Literal); !ok || !types.IsText(expr.Type()) {
				return nil, sql.ErrCharSetIntroducer.New()
			}
			literal, err := expr.Eval(ctx, nil)
			if err != nil {
				return nil, err
			}

			// Internally all strings are `utf8mb4`, so we need to decode the string (which applies the introducer)
			if strLiteral, ok := literal.(string); ok {
				decodedLiteral, ok := charSet.Encoder().Decode(encodings.StringToBytes(strLiteral))
				if !ok {
					return nil, sql.ErrCharSetInvalidString.New(charSet.Name(), strLiteral)
				}
				return expression.NewLiteral(encodings.BytesToString(decodedLiteral), types.CreateLongText(collation)), nil
			} else if byteLiteral, ok := literal.([]byte); ok {
				decodedLiteral, ok := charSet.Encoder().Decode(byteLiteral)
				if !ok {
					return nil, sql.ErrCharSetInvalidString.New(charSet.Name(), strLiteral)
				}
				return expression.NewLiteral(decodedLiteral, types.CreateLongText(collation)), nil
			} else {
				// Should not be possible
				return nil, fmt.Errorf("expression literal returned type `%s` but literal value had type `%T`",
					expr.Type().String(), literal)
			}
		}
		return nil, sql.ErrUnsupportedFeature.New("unary operator: " + e.Operator)
	}
}

func binaryExprToExpression(ctx *sql.Context, be *sqlparser.BinaryExpr) (sql.Expression, error) {
	l, err := ExprToExpression(ctx, be.Left)
	if err != nil {
		return nil, err
	}

	r, err := ExprToExpression(ctx, be.Right)
	if err != nil {
		return nil, err
	}

	operator := strings.ToLower(be.Operator)
	switch operator {
	case
		sqlparser.PlusStr,
		sqlparser.MinusStr,
		sqlparser.MultStr,
		sqlparser.DivStr,
		sqlparser.ShiftLeftStr,
		sqlparser.ShiftRightStr,
		sqlparser.BitAndStr,
		sqlparser.BitOrStr,
		sqlparser.BitXorStr,
		sqlparser.IntDivStr,
		sqlparser.ModStr:

		_, lok := l.(*expression.Interval)
		_, rok := r.(*expression.Interval)
		if lok && be.Operator == "-" {
			return nil, sql.ErrUnsupportedSyntax.New("subtracting from an interval")
		} else if (lok || rok) && be.Operator != "+" && be.Operator != "-" {
			return nil, sql.ErrUnsupportedSyntax.New("only + and - can be used to add or subtract intervals from dates")
		} else if lok && rok {
			return nil, sql.ErrUnsupportedSyntax.New("intervals cannot be added or subtracted from other intervals")
		}

		switch operator {
		case sqlparser.DivStr:
			return expression.NewDiv(l, r), nil
		case sqlparser.ModStr:
			return expression.NewMod(l, r), nil
		case sqlparser.BitAndStr, sqlparser.BitOrStr, sqlparser.BitXorStr, sqlparser.ShiftRightStr, sqlparser.ShiftLeftStr:
			return expression.NewBitOp(l, r, be.Operator), nil
		case sqlparser.IntDivStr:
			return expression.NewIntDiv(l, r), nil
		default:
			return expression.NewArithmetic(l, r, be.Operator), nil
		}

	case sqlparser.JSONExtractOp, sqlparser.JSONUnquoteExtractOp:
		jsonExtract, err := function.NewJSONExtract(l, r)
		if err != nil {
			return nil, err
		}

		if operator == sqlparser.JSONUnquoteExtractOp {
			return function.NewJSONUnquote(jsonExtract), nil
		}
		return jsonExtract, nil

	default:
		return nil, sql.ErrUnsupportedFeature.New(be.Operator)
	}
}

func caseExprToExpression(ctx *sql.Context, e *sqlparser.CaseExpr) (sql.Expression, error) {
	var expr sql.Expression
	var err error

	if e.Expr != nil {
		expr, err = ExprToExpression(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
	}

	var branches []expression.CaseBranch
	for _, w := range e.Whens {
		var cond sql.Expression
		cond, err = ExprToExpression(ctx, w.Cond)
		if err != nil {
			return nil, err
		}

		var val sql.Expression
		val, err = ExprToExpression(ctx, w.Val)
		if err != nil {
			return nil, err
		}

		branches = append(branches, expression.CaseBranch{
			Cond:  cond,
			Value: val,
		})
	}

	var elseExpr sql.Expression
	if e.Else != nil {
		elseExpr, err = ExprToExpression(ctx, e.Else)
		if err != nil {
			return nil, err
		}
	}

	return expression.NewCase(expr, branches, elseExpr), nil
}

func intervalExprToExpression(ctx *sql.Context, e *sqlparser.IntervalExpr) (sql.Expression, error) {
	expr, err := ExprToExpression(ctx, e.Expr)
	if err != nil {
		return nil, err
	}

	return expression.NewInterval(expr, e.Unit), nil
}

func setExprsToExpressions(ctx *sql.Context, e sqlparser.SetVarExprs) ([]sql.Expression, error) {
	res := make([]sql.Expression, len(e))
	for i, setExpr := range e {
		if expr, ok := setExpr.Expr.(*sqlparser.SQLVal); ok && strings.ToLower(setExpr.Name.String()) == "transaction" &&
			(setExpr.Scope == sqlparser.SetScope_Global || setExpr.Scope == sqlparser.SetScope_Session || string(setExpr.Scope) == "") {
			scope := sql.SystemVariableScope_Session
			if setExpr.Scope == sqlparser.SetScope_Global {
				scope = sql.SystemVariableScope_Global
			}
			switch strings.ToLower(expr.String()) {
			case "'isolation level repeatable read'":
				varToSet := expression.NewSystemVar("transaction_isolation", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral("REPEATABLE-READ", types.LongText))
				continue
			case "'isolation level read committed'":
				varToSet := expression.NewSystemVar("transaction_isolation", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral("READ-COMMITTED", types.LongText))
				continue
			case "'isolation level read uncommitted'":
				varToSet := expression.NewSystemVar("transaction_isolation", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral("READ-UNCOMMITTED", types.LongText))
				continue
			case "'isolation level serializable'":
				varToSet := expression.NewSystemVar("transaction_isolation", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral("SERIALIZABLE", types.LongText))
				continue
			case "'read write'":
				varToSet := expression.NewSystemVar("transaction_read_only", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral(false, types.Boolean))
				continue
			case "'read only'":
				varToSet := expression.NewSystemVar("transaction_read_only", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral(true, types.Boolean))
				continue
			}
		}

		innerExpr, err := ExprToExpression(ctx, setExpr.Expr)
		if err != nil {
			return nil, err
		}
		switch setExpr.Scope {
		case sqlparser.SetScope_None:
			colName, err := ExprToExpression(ctx, setExpr.Name)
			if err != nil {
				return nil, err
			}
			res[i] = expression.NewSetField(colName, innerExpr)
		case sqlparser.SetScope_Global:
			varToSet := expression.NewSystemVar(setExpr.Name.String(), sql.SystemVariableScope_Global)
			res[i] = expression.NewSetField(varToSet, innerExpr)
		case sqlparser.SetScope_Persist:
			varToSet := expression.NewSystemVar(setExpr.Name.String(), sql.SystemVariableScope_Persist)
			res[i] = expression.NewSetField(varToSet, innerExpr)
		case sqlparser.SetScope_PersistOnly:
			varToSet := expression.NewSystemVar(setExpr.Name.String(), sql.SystemVariableScope_PersistOnly)
			res[i] = expression.NewSetField(varToSet, innerExpr)
			// TODO reset persist
		//case sqlparser.SetScope_ResetPersist:
		//	varToSet := expression.NewSystemVar(setExpr.Name.String(), sql.SystemVariableScope_ResetPersist)
		//	res[i] = expression.NewSetField(varToSet, innerExpr)
		case sqlparser.SetScope_Session:
			varToSet := expression.NewSystemVar(setExpr.Name.String(), sql.SystemVariableScope_Session)
			res[i] = expression.NewSetField(varToSet, innerExpr)
		case sqlparser.SetScope_User:
			varToSet := expression.NewUserVar(setExpr.Name.String())
			res[i] = expression.NewSetField(varToSet, innerExpr)
		default: // shouldn't happen
			return nil, fmt.Errorf("unknown set scope %v", setExpr.Scope)
		}
	}
	return res, nil
}

func assignmentExprsToExpressions(ctx *sql.Context, e sqlparser.AssignmentExprs) ([]sql.Expression, error) {
	res := make([]sql.Expression, len(e))
	for i, updateExpr := range e {
		colName, err := ExprToExpression(ctx, updateExpr.Name)
		if err != nil {
			return nil, err
		}
		innerExpr, err := ExprToExpression(ctx, updateExpr.Expr)
		if err != nil {
			return nil, err
		}
		res[i] = expression.NewSetField(colName, innerExpr)
	}
	return res, nil
}

func convertShowTableStatus(ctx *sql.Context, s *sqlparser.Show) (sql.Node, error) {
	var filter sql.Expression
	if s.Filter != nil {
		if s.Filter.Filter != nil {
			var err error
			filter, err = ExprToExpression(ctx, s.Filter.Filter)
			if err != nil {
				return nil, err
			}
		} else if s.Filter.Like != "" {
			filter = expression.NewLike(
				expression.NewUnresolvedColumn("Name"),
				expression.NewLiteral(s.Filter.Like, types.LongText),
				nil,
			)
		}
	}

	db := ctx.GetCurrentDatabase()
	if s.Database != "" {
		db = s.Database
	}

	var node sql.Node = plan.NewShowTableStatus(sql.UnresolvedDatabase(db))
	if filter != nil {
		node = plan.NewFilter(filter, node)
	}

	return node, nil
}

func getCurrentUserForDefiner(ctx *sql.Context, definer string) string {
	if definer == "" {
		client := ctx.Session.Client()
		definer = fmt.Sprintf("`%s`@`%s`", client.User, client.Address)
	}
	return definer
}

func getUnresolvedDatabase(ctx *sql.Context, dbName string) (sql.UnresolvedDatabase, error) {
	if dbName == "" {
		dbName = ctx.GetCurrentDatabase()
	}
	udb := sql.UnresolvedDatabase(dbName)
	if dbName == "" {
		return udb, sql.ErrNoDatabaseSelected.New()
	}
	return udb, nil
}

func convertCreateSpatialRefSys(ctx *sql.Context, n *sqlparser.CreateSpatialRefSys) (sql.Node, error) {
	srid, err := strconv.ParseInt(string(n.SRID.Val), 10, 16)
	if err != nil {
		return nil, err
	}

	srsAttr, err := convertSrsAttribute(ctx, n.SrsAttr)
	if err != nil {
		return nil, err
	}

	return plan.NewCreateSpatialRefSys(uint32(srid), n.OrReplace, n.IfNotExists, srsAttr)
}

var ErrMissingMandatoryAttribute = errors.NewKind("missing mandatory attribute %s")
var ErrInvalidName = errors.NewKind("the spatial reference system name can't be an empty string or start or end with whitespace")
var ErrInvalidOrgName = errors.NewKind("the organization name can't be an empty string or start or end with whitespace")

func convertSrsAttribute(ctx *sql.Context, attr *sqlparser.SrsAttribute) (plan.SrsAttribute, error) {
	if attr == nil {
		return plan.SrsAttribute{}, fmt.Errorf("missing attribute")
	}
	if attr.Name == "" {
		return plan.SrsAttribute{}, ErrMissingMandatoryAttribute.New("NAME")
	}
	if unicode.IsSpace(rune(attr.Name[0])) || unicode.IsSpace(rune(attr.Name[len(attr.Name)-1])) {
		return plan.SrsAttribute{}, ErrInvalidName.New()
	}
	// TODO: there are additional rules to validate the attribute definition
	if attr.Definition == "" {
		return plan.SrsAttribute{}, ErrMissingMandatoryAttribute.New("DEFINITION")
	}
	if attr.Organization == "" {
		return plan.SrsAttribute{}, ErrMissingMandatoryAttribute.New("ORGANIZATION NAME")
	}
	if unicode.IsSpace(rune(attr.Organization[0])) || unicode.IsSpace(rune(attr.Organization[len(attr.Organization)-1])) {
		return plan.SrsAttribute{}, ErrInvalidOrgName.New()
	}
	if attr.OrgID == nil {
		return plan.SrsAttribute{}, ErrMissingMandatoryAttribute.New("ORGANIZATION ID")
	}
	orgID, err := strconv.ParseInt(string(attr.OrgID.Val), 10, 16)
	if err != nil {
		return plan.SrsAttribute{}, err
	}
	return plan.SrsAttribute{
		Name:         attr.Name,
		Definition:   attr.Definition,
		Organization: attr.Organization,
		OrgID:        uint32(orgID),
		Description:  attr.Description,
	}, nil
}
