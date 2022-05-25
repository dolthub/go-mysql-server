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
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

var (
	errInvalidDescribeFormat = errors.NewKind("invalid format %q for DESCRIBE, supported formats: %s")

	errInvalidSortOrder = errors.NewKind("invalid sort order: %s")

	ErrPrimaryKeyOnNullField = errors.NewKind("All parts of PRIMARY KEY must be NOT NULL")

	ErrInvalidFrameUnit = errors.NewKind("invalid frame unit")

	ErrFrameEndUnboundedPreceding = errors.NewKind("frame end cannot be unbounded preceding")

	ErrFrameStartUnboundedFollowing = errors.NewKind("frame start cannot be unbounded following")
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

func mustCastNumToInt64(x interface{}) int64 {
	switch v := x.(type) {
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case int64:
		return int64(v)
	case uint64:
		i64 := int64(v)
		if v == uint64(i64) {
			return i64
		}
	}

	panic(fmt.Sprintf("failed to convert to int64: %v", x))
}

// Parse parses the given SQL sentence and returns the corresponding node.
func Parse(ctx *sql.Context, query string) (sql.Node, error) {
	n, _, _, err := parse(ctx, query, false)
	return n, err
}

func ParseOne(ctx *sql.Context, query string) (sql.Node, string, string, error) {
	return parse(ctx, query, true)
}

func parse(ctx *sql.Context, query string, multi bool) (sql.Node, string, string, error) {
	span, ctx := ctx.Span("parse", opentracing.Tag{Key: "query", Value: query})
	defer span.Finish()

	s := strings.TrimSpace(query)
	if strings.HasSuffix(s, ";") {
		s = s[:len(s)-1]
	}

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
			if strings.HasSuffix(parsed, ";") {
				parsed = parsed[:len(parsed)-1]
			}
			remainder = s[ri:]
		}
	}

	if err != nil {
		if goerrors.Is(err, sqlparser.ErrEmpty) {
			ctx.Warn(0, "query was empty after trimming comments, so it will be ignored")
			return plan.Nothing, parsed, remainder, nil
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
	node, err := convertDDL(ctx, createStmt, parseResult.(*sqlparser.DDL))
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
	case *sqlparser.Show:
		// When a query is empty it means it comes from a subquery, as we don't
		// have the query itself in a subquery. Hence, a SHOW could not be
		// parsed.
		if query == "" {
			return nil, sql.ErrUnsupportedFeature.New("SHOW in subquery")
		}
		return convertShow(ctx, n, query)
	case *sqlparser.DDL:
		return convertDDL(ctx, query, n)
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

		return plan.NewStartTransaction("", transChar), nil
	case *sqlparser.Commit:
		return plan.NewCommit(""), nil
	case *sqlparser.Rollback:
		return plan.NewRollback(""), nil
	case *sqlparser.Savepoint:
		return plan.NewCreateSavepoint("", n.Identifier), nil
	case *sqlparser.RollbackSavepoint:
		return plan.NewRollbackSavepoint("", n.Identifier), nil
	case *sqlparser.ReleaseSavepoint:
		return plan.NewReleaseSavepoint("", n.Identifier), nil
	case *sqlparser.BeginEndBlock:
		return convertBeginEndBlock(ctx, n, query)
	case *sqlparser.IfStatement:
		return convertIfBlock(ctx, n)
	case *sqlparser.Call:
		return convertCall(ctx, n)
	case *sqlparser.Declare:
		return convertDeclare(ctx, n)
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
		), nil
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
	}
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
	return plan.NewBeginEndBlock(block), nil
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

func convertUse(n *sqlparser.Use) (sql.Node, error) {
	name := n.DBName.String()
	return plan.NewUse(sql.UnresolvedDatabase(name)), nil
}

func convertSet(ctx *sql.Context, n *sqlparser.Set) (sql.Node, error) {
	// Special case: SET NAMES expands to 3 different system variables. The parser doesn't yet support the optional
	// collation string, which is fine since our support for it is mostly fake anyway.
	// See https://dev.mysql.com/doc/refman/8.0/en/set-names.html
	if isSetNames(n.Exprs) {
		return convertSet(ctx, &sqlparser.Set{
			Exprs: sqlparser.SetVarExprs{
				&sqlparser.SetVarExpr{
					Name: sqlparser.NewColName("character_set_client"),
					Expr: n.Exprs[0].Expr,
				},
				&sqlparser.SetVarExpr{
					Name: sqlparser.NewColName("character_set_connection"),
					Expr: n.Exprs[0].Expr,
				},
				&sqlparser.SetVarExpr{
					Name: sqlparser.NewColName("character_set_results"),
					Expr: n.Exprs[0].Expr,
				},
				// TODO: this should also set the collation_connection to the default collation for the character set named
			},
		})
	}

	// Special case: SET CHARACTER SET (CHARSET) expands to 3 different system variables. Although we do not support very
	// many character sets, changing these variables should not have any effect currently as our character set support is
	// mostly hardcoded to utf8mb4.
	// See https://dev.mysql.com/doc/refman/5.7/en/set-character-set.html.
	if isCharset(n.Exprs) {
		csd, err := ctx.GetSessionVariable(ctx, "character_set_database")
		if err != nil {
			return nil, err
		}

		return convertSet(ctx, &sqlparser.Set{
			Exprs: sqlparser.SetVarExprs{
				&sqlparser.SetVarExpr{
					Name: sqlparser.NewColName("character_set_client"),
					Expr: n.Exprs[0].Expr,
				},
				&sqlparser.SetVarExpr{
					Name: sqlparser.NewColName("character_set_results"),
					Expr: n.Exprs[0].Expr,
				},
				&sqlparser.SetVarExpr{
					Name: sqlparser.NewColName("character_set_connection"),
					Expr: &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte(csd.(string))},
				},
			},
		})
	}

	exprs, err := setExprsToExpressions(ctx, n.Exprs)
	if err != nil {
		return nil, err
	}

	return plan.NewSet(exprs), nil
}

func isSetNames(exprs sqlparser.SetVarExprs) bool {
	if len(exprs) != 1 {
		return false
	}

	return strings.ToLower(exprs[0].Name.String()) == "names"
}

func isCharset(exprs sqlparser.SetVarExprs) bool {
	if len(exprs) != 1 {
		return false
	}

	return strings.ToLower(exprs[0].Name.String()) == "charset"
}

func convertShow(ctx *sql.Context, s *sqlparser.Show, query string) (sql.Node, error) {
	showType := strings.ToLower(s.Type)
	switch showType {
	case "processlist":
		return plan.NewShowProcessList(), nil
	case "create table", "create view":
		var asOfExpression sql.Expression
		if s.ShowTablesOpt != nil && s.ShowTablesOpt.AsOf != nil {
			expression, err := ExprToExpression(ctx, s.ShowTablesOpt.AsOf)
			if err != nil {
				return nil, err
			}
			asOfExpression = expression
		}
		table := tableNameToUnresolvedTableAsOf(s.Table, asOfExpression)
		return plan.NewShowCreateTableWithAsOf(table, showType == "create view", asOfExpression), nil
	case "create database", "create schema":
		return plan.NewShowCreateDatabase(
			sql.UnresolvedDatabase(s.Database),
			s.IfNotExists,
		), nil
	case "create trigger":
		return plan.NewShowCreateTrigger(
			sql.UnresolvedDatabase(s.Table.Qualifier.String()),
			s.Table.Name.String(),
		), nil
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
						expression.NewLiteral(s.ShowTablesOpt.Filter.Like, sql.LongText),
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
	case "create procedure":
		return plan.NewShowCreateProcedure(
			sql.UnresolvedDatabase(s.Table.Qualifier.String()),
			s.Table.Name.String(),
		), nil
	case "procedure status":
		var filter sql.Expression

		node, err := Parse(ctx, "select routine_schema as `Db`, routine_name as `Name`, routine_type as `Type`,"+
			"definer as `Definer`, last_altered as `Modified`, created as `Created`, security_type as `Security_type`,"+
			"routine_comment as `Comment`, character_set_client, collation_connection,"+
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
					expression.NewLiteral(s.Filter.Like, sql.LongText),
					nil,
				)
			}
		}

		if filter != nil {
			node = plan.NewFilter(filter, node)
		}
		return node, nil
	case "function status":
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
					expression.NewLiteral(s.Filter.Like, sql.LongText),
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
			node = plan.NewFilter(filter, node)
		}
		return node, nil
	case "index":
		return plan.NewShowIndexes(plan.NewUnresolvedTable(s.Table.Name.String(), s.Table.Qualifier.String())), nil
	case sqlparser.KeywordString(sqlparser.VARIABLES):
		var likepattern string
		if s.Filter != nil {
			if s.Filter.Filter != nil {
				unsupportedShow := fmt.Sprintf("SHOW VARIABLES WHERE ...")
				return nil, sql.ErrUnsupportedFeature.New(unsupportedShow)
			}
			likepattern = s.Filter.Like
		}
		return plan.NewShowVariables(likepattern), nil
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
						expression.NewLiteral(s.ShowTablesOpt.Filter.Like, sql.LongText),
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
		return plan.NewShowDatabases(), nil
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
				pattern := expression.NewLiteral(s.ShowTablesOpt.Filter.Like, sql.LongText)

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
			unsupportedShow := fmt.Sprintf("SHOW COUNT(*) WARNINGS")
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
	case "table status":
		return convertShowTableStatus(ctx, s)
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
			filterExpr, err := ExprToExpression(ctx, *s.ShowCollationFilterOpt)
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
			return plan.NewFilter(filterExpr, infoSchemaSelect), nil
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
					expression.NewLiteral(s.Filter.Like, sql.LongText),
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
		if s.Scope == sqlparser.GlobalStr {
			return plan.NewShowStatus(plan.ShowStatusModifier_Global), nil
		}

		return plan.NewShowStatus(plan.ShowStatusModifier_Session), nil
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

	if u.Type == sqlparser.UnionAllStr {
		return plan.NewUnion(left, right), nil
	} else { // default is DISTINCT (either explicit or implicit)
		// TODO: this creates redundant Distinct nodes that we can't easily remove after the fact. With this construct,
		//  we can't in all cases tell the difference between `union distinct (select ...)` and
		//  `union (select distinct ...)`. We need something like a Distinct property on Union nodes to be able to prune
		//  redundant Distinct nodes and thereby avoid doing extra work.
		return plan.NewDistinct(plan.NewUnion(left, right)), nil
	}
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

	if len(s.OrderBy) != 0 {
		node, err = orderByToSort(ctx, s.OrderBy, node)
		if err != nil {
			return nil, err
		}
	}

	// Limit must wrap offset, and not vice-versa, so that skipped rows don't count toward the returned row count.
	if s.Limit != nil && s.Limit.Offset != nil {
		node, err = offsetToOffset(ctx, s.Limit.Offset, node)
		if err != nil {
			return nil, err
		}
	}

	if s.Limit != nil {
		node, err = limitToLimit(ctx, s.Limit.Rowcount, node)
		if err != nil {
			return nil, err
		}

		if s.CalcFoundRows {
			node.(*plan.Limit).CalcFoundRows = true
		}
	} else if ok, val := sql.HasDefaultValue(ctx, ctx.Session, "sql_select_limit"); !ok {
		limit := mustCastNumToInt64(val)
		node = plan.NewLimit(expression.NewLiteral(limit, sql.Int64), node)
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
			vars[i] = expression.NewProcedureParam(val.String())
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

func convertDDL(ctx *sql.Context, query string, c *sqlparser.DDL) (sql.Node, error) {
	switch strings.ToLower(c.Action) {
	case sqlparser.CreateStr:
		if c.TriggerSpec != nil {
			return convertCreateTrigger(ctx, query, c)
		}
		if c.ProcedureSpec != nil {
			return convertCreateProcedure(ctx, query, c)
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
		if len(c.FromViews) != 0 {
			return convertDropView(ctx, c)
		}
		return convertDropTable(ctx, c)
	case sqlparser.AlterStr:
		return convertAlterTable(ctx, c)
	case sqlparser.RenameStr:
		return convertRenameTable(ctx, c)
	case sqlparser.TruncateStr:
		return convertTruncateTable(ctx, c)
	default:
		return nil, sql.ErrUnsupportedSyntax.New(sqlparser.String(c))
	}
}

func convertMultiAlterDDL(ctx *sql.Context, query string, c *sqlparser.MultiAlterDDL) (sql.Node, error) {
	statementsLen := len(c.Statements)
	if statementsLen == 1 {
		return convertDDL(ctx, query, c.Statements[0])
	}
	statements := make([]sql.Node, statementsLen)
	var err error
	for i := 0; i < statementsLen; i++ {
		statements[i], err = convertDDL(ctx, query, c.Statements[i])
		if err != nil {
			return nil, err
		}
	}
	return plan.NewBlock(statements), nil
}

func convertDBDDL(ctx *sql.Context, c *sqlparser.DBDDL) (sql.Node, error) {
	switch strings.ToLower(c.Action) {
	case sqlparser.CreateStr:
		if len(c.CharsetCollate) > 0 {
			ctx.Session.Warn(&sql.Warning{
				Level:   "Warning",
				Code:    mysql.ERNotSupportedYet,
				Message: fmt.Sprintf("Setting CHARACTER SET, COLLATION and ENCRYPTION are not supported yet"),
			})
		}
		return plan.NewCreateDatabase(c.DBName, c.IfNotExists), nil
	case sqlparser.DropStr:
		return plan.NewDropDatabase(c.DBName, c.IfExists), nil
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
		c.TriggerSpec.Definer,
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
		internalTyp, err := sql.ColumnTypeToType(&param.Type)
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
	return plan.NewCall(c.FuncName, params), nil
}

func convertDeclare(ctx *sql.Context, d *sqlparser.Declare) (sql.Node, error) {
	if d.Condition != nil {
		return convertDeclareCondition(ctx, d)
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
			number, err := strconv.ParseUint(string(info.Value.Val), 10, 16)
			if err != nil || number == 0 {
				// We use our own error instead
				return nil, fmt.Errorf("invalid value '%s' for signal condition information item MYSQL_ERRNO", string(info.Value.Val))
			}
			si.IntValue = int64(number)
		} else if si.ConditionItemName == plan.SignalConditionItemName_MessageText {
			val := string(info.Value.Val)
			if len(val) > 128 {
				return nil, fmt.Errorf("signal condition information item MESSAGE_TEXT has max length of 128")
			}
			si.StrValue = val
		} else {
			val := string(info.Value.Val)
			if len(val) > 64 {
				return nil, fmt.Errorf("signal condition information item %s has max length of 64", strings.ToUpper(string(si.ConditionItemName)))
			}
			si.StrValue = val
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

func convertRenameTable(ctx *sql.Context, ddl *sqlparser.DDL) (sql.Node, error) {
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

	return plan.NewRenameTable(sql.UnresolvedDatabase(""), fromTables, toTables), nil
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
				c.Database = table.Database()
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
				database := table.Database()
				if database == "" {
					database = ctx.GetCurrentDatabase()
				}
				return plan.NewAlterDropForeignKey(database, table.Name(), c.Name), nil
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
		switch strings.ToLower(ddl.ColumnAction) {
		case sqlparser.AddStr:
			sch, err := TableSpecToSchema(ctx, ddl.TableSpec)
			if err != nil {
				return nil, err
			}
			return plan.NewAddColumn(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), tableNameToUnresolvedTable(ddl.Table), sch.Schema[0], columnOrderToColumnOrder(ddl.ColumnOrder)), nil
		case sqlparser.DropStr:
			return plan.NewDropColumn(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), tableNameToUnresolvedTable(ddl.Table), ddl.Column.String()), nil
		case sqlparser.RenameStr:
			return plan.NewRenameColumn(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), tableNameToUnresolvedTable(ddl.Table), ddl.Column.String(), ddl.ToColumn.String()), nil
		case sqlparser.ModifyStr, sqlparser.ChangeStr:
			sch, err := TableSpecToSchema(nil, ddl.TableSpec)
			if err != nil {
				return nil, err
			}
			return plan.NewModifyColumn(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), tableNameToUnresolvedTable(ddl.Table), ddl.Column.String(), sch.Schema[0], columnOrderToColumnOrder(ddl.ColumnOrder)), nil
		}
	}
	if ddl.AutoIncSpec != nil {
		return convertAlterAutoIncrement(ddl)
	}
	if ddl.DefaultSpec != nil {
		return convertAlterDefault(ctx, ddl)
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

		columns := make([]sql.IndexColumn, len(ddl.IndexSpec.Columns))
		for i, col := range ddl.IndexSpec.Columns {
			if col.Length != nil {
				if col.Length.Type == sqlparser.IntVal {
					length, err := strconv.ParseInt(string(col.Length.Val), 10, 64)
					if err != nil {
						return nil, err
					}
					if length < 1 {
						return nil, sql.ErrInvalidIndexPrefix.New(length)
					}
				}
			}
			columns[i] = sql.IndexColumn{
				Name:   col.Column.String(),
				Length: 0,
			}
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

		return plan.NewAlterCreateIndex(sql.UnresolvedDatabase(ddl.Table.Qualifier.String()), table, ddl.IndexSpec.ToName.String(), using, constraint, columns, comment), nil
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

	var fkDefs []*sql.ForeignKeyConstraint
	var chDefs []*sql.CheckConstraint
	for _, unknownConstraint := range c.TableSpec.Constraints {
		parsedConstraint, err := convertConstraintDefinition(ctx, unknownConstraint)
		if err != nil {
			return nil, err
		}
		switch constraint := parsedConstraint.(type) {
		case *sql.ForeignKeyConstraint:
			constraint.Database = c.Table.Qualifier.String()
			constraint.Table = c.Table.Name.String()
			if constraint.Database == "" {
				constraint.Database = ctx.GetCurrentDatabase()
			}
			fkDefs = append(fkDefs, constraint)
		case *sql.CheckConstraint:
			chDefs = append(chDefs, constraint)
		default:
			return nil, sql.ErrUnknownConstraintDefinition.New(unknownConstraint.Name, unknownConstraint)
		}
	}

	var idxDefs []*plan.IndexDefinition
	for _, idxDef := range c.TableSpec.Indexes {
		constraint := sql.IndexConstraint_None
		if idxDef.Info.Primary {
			constraint = sql.IndexConstraint_Primary
		} else if idxDef.Info.Unique {
			constraint = sql.IndexConstraint_Unique
		} else if idxDef.Info.Spatial {
			constraint = sql.IndexConstraint_Spatial
		} else if idxDef.Info.Fulltext {
			// TODO: We do not support FULLTEXT indexes or keys
			return nil, sql.ErrUnsupportedFeature.New("fulltext keys are unsupported")
		}

		columns := make([]sql.IndexColumn, len(idxDef.Columns))
		for i, col := range idxDef.Columns {
			if col.Length != nil {
				if col.Length.Type == sqlparser.IntVal {
					length, err := strconv.ParseInt(string(col.Length.Val), 10, 64)
					if err != nil {
						return nil, err
					}
					if length < 1 {
						return nil, sql.ErrInvalidIndexPrefix.New(length)
					}
				}
			}
			columns[i] = sql.IndexColumn{
				Name:   col.Column.String(),
				Length: 0,
			}
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

	for _, colDef := range c.TableSpec.Columns {
		if colDef.Type.KeyOpt == colKeyFulltextKey {
			return nil, sql.ErrUnsupportedFeature.New("fulltext keys are unsupported")
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

	qualifier := c.Table.Qualifier.String()

	schema, err := TableSpecToSchema(nil, c.TableSpec)
	if err != nil {
		return nil, err
	}

	tableSpec := &plan.TableSpec{
		Schema:  schema,
		IdxDefs: idxDefs,
		FkDefs:  fkDefs,
		ChDefs:  chDefs,
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

	return plan.NewCreateView(
		sql.UnresolvedDatabase(""), c.ViewSpec.ViewName.Name.String(), []string{}, queryAlias, c.OrReplace), nil
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

	columnWithDefaultValues := make(map[int]bool)

	if e, ok := src.(*plan.Values); ok {
		for i, tuple := range e.ExpressionTuples {
			var needCols []sql.Expression
			for j, s := range tuple {
				if _, ok := s.(*expression.DefaultColumn); ok {
					columnWithDefaultValues[j] = true
				} else {
					needCols = append(needCols, s)
				}
			}

			// Only re-assign if found column with default values
			if len(columnWithDefaultValues) == 0 {
				break
			}

			e.ExpressionTuples[i] = needCols
		}
	}

	var columns []string
	if len(columnWithDefaultValues) > 0 {
		for i, c := range columnsToStrings(i.Columns) {
			if _, found := columnWithDefaultValues[i]; !found {
				columns = append(columns, c)
			}
		}
	} else {
		columns = columnsToStrings(i.Columns)
	}

	return plan.NewInsertInto(sql.UnresolvedDatabase(i.Table.Qualifier.String()), tableNameToUnresolvedTable(i.Table), src, isReplace, columns, onDupExprs, ignore), nil
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

	return plan.NewDeleteFrom(node), nil
}

func convertUpdate(ctx *sql.Context, d *sqlparser.Update) (sql.Node, error) {
	node, err := tableExprsToTable(ctx, d.TableExprs)
	if err != nil {
		return nil, err
	}

	// If the top level node can store comments and one was provided, store it.
	if cn, ok := node.(sql.CommentedNode); ok && len(d.Comments) > 0 {
		node = cn.WithComment(string(d.Comments[0]))
	}

	updateExprs, err := assignmentExprsToExpressions(ctx, d.Exprs)
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

	return plan.NewUpdate(node, updateExprs), nil
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

	ld := plan.NewLoadData(bool(d.Local), d.Infile, unresolvedTable, columnsToStrings(d.Columns), d.Fields, d.Lines, ignoreNumVal)

	return plan.NewInsertInto(sql.UnresolvedDatabase(d.Table.Qualifier.String()), tableNameToUnresolvedTable(d.Table), ld, false, ld.ColumnNames, nil, false), nil
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
func TableSpecToSchema(ctx *sql.Context, tableSpec *sqlparser.TableSpec) (sql.PrimaryKeySchema, error) {
	var schema sql.Schema
	for _, cd := range tableSpec.Columns {
		column, err := columnDefinitionToColumn(ctx, cd, tableSpec.Indexes)
		if err != nil {
			return sql.PrimaryKeySchema{}, err
		}

		if column.PrimaryKey && bool(cd.Type.Null) {
			return sql.PrimaryKeySchema{}, ErrPrimaryKeyOnNullField.New()
		}

		schema = append(schema, column)
	}

	return sql.NewPrimaryKeySchema(schema, getPkOrdinals(tableSpec)...), nil
}

// columnDefinitionToColumn returns the sql.Column for the column definition given, as part of a create table statement.
func columnDefinitionToColumn(ctx *sql.Context, cd *sqlparser.ColumnDefinition, indexes []*sqlparser.IndexDefinition) (*sql.Column, error) {
	internalTyp, err := sql.ColumnTypeToType(&cd.Type)
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
		if uint32(sridVal) != sql.CartesianSRID && uint32(sridVal) != sql.GeoSpatialSRID {
			return nil, sql.ErrUnsupportedFeature.New("unsupported SRID value")
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
	// The literal and expression distinction seems to be decided by the presence of parentheses, even for defaults like NOW() vs (NOW())
	_, isExpr := defaultExpr.(*sqlparser.ParenExpr)
	// A literal will never have children, thus we can also check for that.
	isExpr = isExpr || len(parsedExpr.Children()) != 0
	return ExpressionToColumnDefaultValue(ctx, parsedExpr, !isExpr)
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
		default:
			// all privileges have been implemented, so if we hit the default something bad has happened
			panic("given privilege type parses but is not implemented")
		}
		planPrivs[i] = plan.Privilege{
			Type:    privType,
			Columns: privilege.Columns,
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
			if user.Auth1.Plugin == "mysql_native_password" && len(user.Auth1.Password) > 0 {
				authUser.Auth1 = plan.AuthenticationMysqlNativePassword(user.Auth1.Password)
			} else if user.Auth1.Plugin == "" && len(user.Auth1.Password) > 0 {
				authUser.Auth1 = plan.NewDefaultAuthentication(user.Auth1.Password)
			} else {
				return nil, fmt.Errorf(`the given authentication format is not yet supported`)
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
	), nil
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
		return nil, sql.ErrUnsupportedFeature.New("zero tables in FROM")
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

		return expression.NewUnresolvedTableFunction(t.Name, exprs), nil

	case *sqlparser.JoinTableExpr:
		return joinTableExpr(ctx, t)

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
		return plan.NewLeftJoin(left, right, cond), nil
	case sqlparser.RightJoinStr:
		return plan.NewRightJoin(left, right, cond), nil
	default:
		return nil, sql.ErrUnsupportedFeature.New("Join type " + t.Join)
	}
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
) (*plan.Offset, error) {
	rowCount, err := ExprToExpression(ctx, offset)
	if err != nil {
		return nil, err
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
		if !sql.IsInteger(e.Type()) {
			return nil, sql.ErrUnsupportedFeature.New(errStr)
		}
	}
	nl, ok := e.(*expression.Literal)
	if !ok || !sql.IsInteger(nl.Type()) {
		return nil, sql.ErrUnsupportedFeature.New(errStr)
	} else {
		i64, err := sql.Int64.Convert(nl.Value())
		if err != nil {
			return nil, sql.ErrUnsupportedFeature.New(errStr)
		}
		return expression.NewLiteral(i64, sql.Int64), nil
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
			if l, ok := ge.(*expression.Literal); ok && sql.IsNumber(l.Type()) {
				if i64, err := sql.Int64.Convert(l.Value()); err == nil {
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
	// The literal and expression distinction seems to be decided by the presence of parentheses, even for defaults like NOW() vs (NOW())
	// 2+2 would evaluate to a literal under the parentheses check, but will have children due to being an Arithmetic expression, thus we check for children.
	return ExpressionToColumnDefaultValue(ctx, parsedExpr, len(parsedExpr.Children()) == 0 && !strings.HasPrefix(exprStr, "("))
}

// ExpressionToColumnDefaultValue takes in an Expression and returns the equivalent ColumnDefaultValue if the expression
// is valid for a default value. If the expression represents a literal (and not an expression that returns a literal, so "5"
// rather than "(5)"), then the parameter "isLiteral" should be true.
func ExpressionToColumnDefaultValue(ctx *sql.Context, inputExpr sql.Expression, isLiteral bool) (*sql.ColumnDefaultValue, error) {
	return sql.NewColumnDefaultValue(inputExpr, nil, isLiteral, true)
}

// MustStringToColumnDefaultValue is used for creating default values on tables that do not go through the analyzer. Does not handle
// function nor column references.
func MustStringToColumnDefaultValue(ctx *sql.Context, exprStr string, outType sql.Type, nullable bool) *sql.ColumnDefaultValue {
	expr, err := StringToColumnDefaultValue(ctx, exprStr)
	if err != nil {
		panic(err)
	}
	expr, err = sql.NewColumnDefaultValue(expr.Expression, outType, expr.IsLiteral(), nullable)
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
		return convertVal(v)
	case sqlparser.BoolVal:
		return expression.NewLiteral(bool(v), sql.Boolean), nil
	case *sqlparser.NullVal:
		return expression.NewLiteral(nil, sql.Null), nil
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
			if len(exprs) != 1 {
				return nil, sql.ErrUnsupportedSyntax.New("more than one expression in COUNT")
			}

			return aggregation.NewCountDistinct(exprs[0]), nil
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
		if v.Separator != "" {
			separatorS = v.Separator
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

		return aggregation.NewGroupConcat(v.Distinct, sortFields, separatorS, exprs, int(groupConcatMaxLen))
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
	case *sqlparser.ConvertExpr:
		expr, err := ExprToExpression(ctx, v.Expr)
		if err != nil {
			return nil, err
		}

		return expression.NewConvert(expr, v.Type.Type), nil
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
		// TODO: handle collation
		return ExprToExpression(ctx, v.Expr)
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

		return plan.NewExistsSubquery(subqueryExp), nil
	case *sqlparser.TimestampFuncExpr:
		var (
			unit  sql.Expression
			expr1 sql.Expression
			expr2 sql.Expression
			err   error
		)

		unit = expression.NewLiteral(v.Unit, sql.LongText)
		expr1, err = ExprToExpression(ctx, v.Expr1)
		expr2, err = ExprToExpression(ctx, v.Expr2)

		if v.Name == "timestampdiff" {
			return function.NewTimestampDiff(unit, expr1, expr2), err
		} else if v.Name == "timestampadd" {
			return nil, fmt.Errorf("TIMESTAMPADD() not supported")
		}
		return nil, nil
	}
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
	return sql.NewWindowDefinition(partitions, sortFields, frame, def.NameRef.Lowered(), def.Name.Lowered()), nil
}

func isAggregateFunc(v *sqlparser.FuncExpr) bool {
	switch v.Name.Lowered() {
	case "first", "last", "count", "sum", "avg", "max", "min",
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
		return expression.NewLiteral(int8(i8), sql.Int8), nil
	}
	if ui8, err := strconv.ParseUint(value, base, 8); err == nil {
		return expression.NewLiteral(uint8(ui8), sql.Uint8), nil
	}
	if i16, err := strconv.ParseInt(value, base, 16); err == nil {
		return expression.NewLiteral(int16(i16), sql.Int16), nil
	}
	if ui16, err := strconv.ParseUint(value, base, 16); err == nil {
		return expression.NewLiteral(uint16(ui16), sql.Uint16), nil
	}
	if i32, err := strconv.ParseInt(value, base, 32); err == nil {
		return expression.NewLiteral(int32(i32), sql.Int32), nil
	}
	if ui32, err := strconv.ParseUint(value, base, 32); err == nil {
		return expression.NewLiteral(uint32(ui32), sql.Uint32), nil
	}
	if i64, err := strconv.ParseInt(value, base, 64); err == nil {
		return expression.NewLiteral(int64(i64), sql.Int64), nil
	}

	ui64, err := strconv.ParseUint(value, base, 64)
	if err != nil {
		return nil, err
	}

	return expression.NewLiteral(uint64(ui64), sql.Uint64), nil
}

func convertVal(v *sqlparser.SQLVal) (sql.Expression, error) {
	switch v.Type {
	case sqlparser.StrVal:
		return expression.NewLiteral(string(v.Val), sql.LongText), nil
	case sqlparser.IntVal:
		return convertInt(string(v.Val), 10)
	case sqlparser.FloatVal:
		val, err := strconv.ParseFloat(string(v.Val), 64)
		if err != nil {
			return nil, err
		}
		return expression.NewLiteral(val, sql.Float64), nil
	case sqlparser.HexNum:
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
		return expression.NewLiteral(dst, sql.LongBlob), nil
	case sqlparser.HexVal:
		val, err := v.HexDecode()
		if err != nil {
			return nil, err
		}
		return expression.NewLiteral(val, sql.LongBlob), nil
	case sqlparser.ValArg:
		return expression.NewBindVar(strings.TrimPrefix(string(v.Val), ":")), nil
	case sqlparser.BitVal:
		if len(v.Val) == 0 {
			return expression.NewLiteral(0, sql.Uint64), nil
		}

		res, err := strconv.ParseUint(string(v.Val), 2, 64)
		if err != nil {
			return nil, err
		}

		return expression.NewLiteral(res, sql.Uint64), nil
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
	case "_binary ":
		// Charset introducers do not operate as CONVERT, they just state how a string should be interpreted.
		// TODO: if we encounter a non-string, do something other than just return
		expr, err := ExprToExpression(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
		if exprLiteral, ok := expr.(*expression.Literal); ok && sql.IsTextOnly(exprLiteral.Type()) {
			return expression.NewLiteral(exprLiteral.Value(), sql.LongBlob), nil
		}
		return expr, nil
	case "_utf8mb4 ", "_utf8mb3 ", "_utf8 ":
		expr, err := ExprToExpression(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
		// must be string type
		if !sql.IsText(expr.Type()) {
			return nil, sql.ErrInvalidType.New(expr.Type().String() + " after character set introducer")
		}
		return expr, nil
	default:
		if strings.HasPrefix(strings.ToLower(e.Operator), "_") {
			return nil, sql.ErrUnsupportedFeature.New("unsupported character set: " + e.Operator)
		}
		return nil, sql.ErrUnsupportedFeature.New("unary operator: " + e.Operator)
	}
}

func binaryExprToExpression(ctx *sql.Context, be *sqlparser.BinaryExpr) (sql.Expression, error) {
	switch strings.ToLower(be.Operator) {
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

		l, err := ExprToExpression(ctx, be.Left)
		if err != nil {
			return nil, err
		}

		r, err := ExprToExpression(ctx, be.Right)
		if err != nil {
			return nil, err
		}

		_, lok := l.(*expression.Interval)
		_, rok := r.(*expression.Interval)
		if lok && be.Operator == "-" {
			return nil, sql.ErrUnsupportedSyntax.New("subtracting from an interval")
		} else if (lok || rok) && be.Operator != "+" && be.Operator != "-" {
			return nil, sql.ErrUnsupportedSyntax.New("only + and - can be used to add or subtract intervals from dates")
		} else if lok && rok {
			return nil, sql.ErrUnsupportedSyntax.New("intervals cannot be added or subtracted from other intervals")
		}

		return expression.NewArithmetic(l, r, be.Operator), nil
	case
		sqlparser.JSONExtractOp,
		sqlparser.JSONUnquoteExtractOp:
		return nil, sql.ErrUnsupportedFeature.New(fmt.Sprintf("(%s) JSON operators not supported", be.Operator))

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
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral("REPEATABLE-READ", sql.LongText))
				continue
			case "'isolation level read committed'":
				varToSet := expression.NewSystemVar("transaction_isolation", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral("READ-COMMITTED", sql.LongText))
				continue
			case "'isolation level read uncommitted'":
				varToSet := expression.NewSystemVar("transaction_isolation", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral("READ-UNCOMMITTED", sql.LongText))
				continue
			case "'isolation level serializable'":
				varToSet := expression.NewSystemVar("transaction_isolation", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral("SERIALIZABLE", sql.LongText))
				continue
			case "'read write'":
				varToSet := expression.NewSystemVar("transaction_read_only", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral(false, sql.Boolean))
				continue
			case "'read only'":
				varToSet := expression.NewSystemVar("transaction_read_only", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral(true, sql.Boolean))
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
				expression.NewLiteral(s.Filter.Like, sql.LongText),
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
