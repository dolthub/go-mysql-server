package planbuilder

import (
	"fmt"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *PlanBuilder) buildAnalyze(inScope *scope, n *ast.Analyze, query string) (outScope *scope) {
	outScope = inScope.push()
	names := make([]sql.DbTable, len(n.Tables))
	for i, table := range n.Tables {
		names[i] = sql.DbTable{Db: table.Qualifier.String(), Table: table.Name.String()}
	}
	outScope.node = plan.NewAnalyze(names)
	return
}

func (b *PlanBuilder) buildShow(inScope *scope, s *ast.Show, query string) (outScope *scope) {
	outScope = inScope.push()
	showType := strings.ToLower(s.Type)
	switch showType {
	case "processlist":
		outScope.node = plan.NewShowProcessList()
	case ast.CreateTableStr, "create view":
		return b.buildShowTable(inScope, s, showType)
	case "create database", "create schema":
		return b.buildShowDatabase(inScope, s)
	case ast.CreateTriggerStr:
		return b.buildShowTrigger(inScope, s)
	case ast.CreateProcedureStr:
		return b.buildShowProcedure(inScope, s)
	case ast.CreateEventStr:
		return b.buildShowEvent(inScope, s)
	case "triggers":
		return b.buildShowAllTriggers(inScope, s)
	case "events":
		return b.buildShowAllEvents(inScope, s)
	case ast.ProcedureStatusStr:
		return b.buildShowProcedureStatus(inScope, s)
	case ast.FunctionStatusStr:
		return b.buildShowFunctionStatus(inScope, s)
	case ast.TableStatusStr:
		return b.buildShowTableStatus(inScope, s)
	case "index":
		return b.buildShowIndex(inScope, s)
	case ast.KeywordString(ast.VARIABLES):
		return b.buildShowVariables(inScope, s)
	case ast.KeywordString(ast.TABLES):
		return b.buildShowAllTables(inScope, s)
	case ast.KeywordString(ast.DATABASES), ast.KeywordString(ast.SCHEMAS):
		return b.buildShowAllDatabases(inScope, s)
	case ast.KeywordString(ast.FIELDS), ast.KeywordString(ast.COLUMNS):
		return b.buildShowAllColumns(inScope, s)
	case ast.KeywordString(ast.WARNINGS):
		return b.buildShowWarnings(inScope, s)
	case ast.KeywordString(ast.COLLATION):
		return b.buildShowCollation(inScope, s)
	case ast.KeywordString(ast.CHARSET):
		return b.buildShowCharset(inScope, s)
	case ast.KeywordString(ast.ENGINES):
		return b.buildShowEngines(inScope, s)
	case ast.KeywordString(ast.STATUS):
		return b.buildShowStatus(inScope, s)
	case "replica status":
		outScope.node = plan.NewShowReplicaStatus()
	default:
		unsupportedShow := fmt.Sprintf("SHOW %s", s.Type)
		b.handleErr(sql.ErrUnsupportedFeature.New(unsupportedShow))
	}
	return
}

func (b *PlanBuilder) buildShowTable(inScope *scope, s *ast.Show, showType string) (outScope *scope) {
	outScope = inScope.push()
	var asOfLit interface{}
	var asOfExpr sql.Expression
	if s.ShowTablesOpt != nil && s.ShowTablesOpt.AsOf != nil {
		var err error
		asOfExpr = b.buildScalar(inScope, s.ShowTablesOpt.AsOf)
		asOfLit, err = asOfExpr.Eval(b.ctx, nil)
		if err != nil {
			b.handleErr(err)
		}
	}

	db := s.Database
	if db == "" {
		db = s.Table.Qualifier.String()
	}
	if db == "" {
		db = b.currentDb().Name()
	}

	rt := b.resolveTable(s.Table.Name.String(), db, asOfLit)

	database, err := b.cat.Database(b.ctx, b.ctx.GetCurrentDatabase())
	if err != nil {
		b.handleErr(err)
	}

	if privilegedDatabase, ok := database.(mysql_db.PrivilegedDatabase); ok {
		database = privilegedDatabase.Unwrap()
	}
	outScope.node = plan.NewShowCreateTableWithAsOf(rt, showType == "create view", asOfExpr)
	return
}

func (b *PlanBuilder) buildShowDatabase(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	outScope = inScope.push()
	outScope.node = plan.NewShowCreateDatabase(
		sql.UnresolvedDatabase(s.Database),
		s.IfNotExists,
	)
	return
}

func (b *PlanBuilder) buildShowTrigger(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	db, err := b.cat.Database(b.ctx, s.Table.Qualifier.String())
	if err != nil {
		b.handleErr(err)
	}
	outScope.node = plan.NewShowCreateTrigger(db, s.Table.Name.String())
	return
}

func (b *PlanBuilder) buildShowAllTriggers(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	var dbName string
	var filter sql.Expression

	if s.ShowTablesOpt != nil && s.ShowTablesOpt.AsOf != nil {
		dbName = s.ShowTablesOpt.DbName
		if s.ShowTablesOpt.Filter != nil {
			if s.ShowTablesOpt.Filter.Filter != nil {
				filter = b.buildScalar(inScope, s.ShowTablesOpt.Filter.Filter)
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

	outScope.node = node
	return
}

func (b *PlanBuilder) buildShowEvent(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	db, err := b.cat.Database(b.ctx, s.Table.Qualifier.String())
	if err != nil {
		b.handleErr(err)
	}
	outScope.node = plan.NewShowCreateEvent(db, s.Table.Name.String())
	return
}

func (b *PlanBuilder) buildShowAllEvents(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	var dbName string
	var filter sql.Expression
	if s.ShowTablesOpt != nil && s.ShowTablesOpt.AsOf != nil {
		dbName = s.ShowTablesOpt.DbName
		if s.ShowTablesOpt.Filter != nil {
			if s.ShowTablesOpt.Filter.Filter != nil {
				filter = b.buildScalar(inScope, s.ShowTablesOpt.Filter.Filter)
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

	outScope.node = node
	return
}

func (b *PlanBuilder) buildShowProcedure(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	db, err := b.cat.Database(b.ctx, s.Table.Qualifier.String())
	if err != nil {
		b.handleErr(err)
	}
	outScope.node = plan.NewShowCreateProcedure(db, s.Table.Name.String())
	return
}

func (b *PlanBuilder) buildShowProcedureStatus(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	var filter sql.Expression

	node, err := Parse(b.ctx, b.cat, "select routine_schema as `Db`, routine_name as `Name`, routine_type as `Type`,"+
		"definer as `Definer`, last_altered as `Modified`, created as `Created`, security_type as `Security_type`,"+
		"routine_comment as `Comment`, CHARACTER_SET_CLIENT as `character_set_client`, COLLATION_CONNECTION as `collation_connection`,"+
		"database_collation as `Database Collation` from information_schema.routines where routine_type = 'PROCEDURE'")
	if err != nil {
		b.handleErr(err)
	}

	if s.Filter != nil {
		if s.Filter.Filter != nil {
			filter = b.buildScalar(inScope, s.Filter.Filter)
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
	outScope.node = node
	return
}

func (b *PlanBuilder) buildShowFunctionStatus(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	var filter sql.Expression
	var node sql.Node
	if s.Filter != nil {
		if s.Filter.Filter != nil {
			filter = b.buildScalar(inScope, s.Filter.Filter)
		} else if s.Filter.Like != "" {
			filter = expression.NewLike(
				expression.NewUnresolvedColumn("Name"),
				expression.NewLiteral(s.Filter.Like, types.LongText),
				nil,
			)
		}
	}

	node, err := Parse(b.ctx, b.cat, "select routine_schema as `Db`, routine_name as `Name`, routine_type as `Type`,"+
		"definer as `Definer`, last_altered as `Modified`, created as `Created`, security_type as `Security_type`,"+
		"routine_comment as `Comment`, character_set_client, collation_connection,"+
		"database_collation as `Database Collation` from information_schema.routines where routine_type = 'FUNCTION'")
	if err != nil {
		b.handleErr(err)
	}

	if filter != nil {
		node = plan.NewHaving(filter, node)
	}
	outScope.node = node
	return
}

func (b *PlanBuilder) buildShowTableStatus(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	var filter sql.Expression
	if s.Filter != nil {
		if s.Filter.Filter != nil {
			filter = b.buildScalar(inScope, s.Filter.Filter)
		} else if s.Filter.Like != "" {
			filter = expression.NewLike(
				expression.NewUnresolvedColumn("Name"),
				expression.NewLiteral(s.Filter.Like, types.LongText),
				nil,
			)
		}
	}

	db := b.ctx.GetCurrentDatabase()
	if s.Database != "" {
		db = s.Database
	}

	var node sql.Node = plan.NewShowTableStatus(sql.UnresolvedDatabase(db))
	if filter != nil {
		node = plan.NewFilter(filter, node)
	}

	outScope.node = node
	return
}
func (b *PlanBuilder) buildShowIndex(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	outScope.node = plan.NewShowIndexes(plan.NewUnresolvedTable(s.Table.Name.String(), s.Table.Qualifier.String()))
	return
}

func (b *PlanBuilder) buildShowVariables(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	var filter sql.Expression
	var like sql.Expression
	if s.Filter != nil {
		if s.Filter.Filter != nil {
			filter = b.buildScalar(inScope, s.Filter.Filter)
			filter, _, _ = transform.Expr(filter, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
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

	outScope.node = plan.NewShowVariables(filter, strings.ToLower(s.Scope) == "global")
	return
}

func (b *PlanBuilder) buildShowAllTables(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	var dbName string
	var filter sql.Expression
	var asOf sql.Expression
	var full bool

	if s.ShowTablesOpt != nil && s.ShowTablesOpt.AsOf != nil {
		dbName = s.ShowTablesOpt.DbName
		if dbName == "" {
			dbName = b.ctx.GetCurrentDatabase()
		}
		full = s.Full

		if s.ShowTablesOpt.Filter != nil {
			if s.ShowTablesOpt.Filter.Filter != nil {
				filter = b.buildScalar(inScope, s.ShowTablesOpt.Filter.Filter)
			} else if s.ShowTablesOpt.Filter.Like != "" {
				filter = expression.NewLike(
					expression.NewUnresolvedColumn(fmt.Sprintf("Tables_in_%s", dbName)),
					expression.NewLiteral(s.ShowTablesOpt.Filter.Like, types.LongText),
					nil,
				)
			}
		}

		if s.ShowTablesOpt.AsOf != nil {
			asOf = b.buildScalar(inScope, s.ShowTablesOpt.AsOf)
		}
	}

	var node sql.Node = plan.NewShowTables(sql.UnresolvedDatabase(dbName), full, asOf)
	if filter != nil {
		node = plan.NewFilter(filter, node)
	}

	outScope.node = node
	return
}

func (b *PlanBuilder) buildShowAllDatabases(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	var node sql.Node = plan.NewShowDatabases()
	var filter sql.Expression
	if s.Filter != nil {
		if s.Filter.Filter != nil {
			filter = b.buildScalar(inScope, s.Filter.Filter)
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
	outScope.node = node
	return
}

func (b *PlanBuilder) buildShowAllColumns(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	full := s.Full
	var table sql.Node
	{
		var asOfLit interface{}
		var asOfExpr sql.Expression
		if s.ShowTablesOpt != nil && s.ShowTablesOpt.AsOf != nil {
			var err error
			asOfExpr = b.buildScalar(inScope, s.ShowTablesOpt.AsOf)
			asOfLit, err = asOfExpr.Eval(b.ctx, nil)
			if err != nil {
				b.handleErr(err)
			}
		}

		db := s.Database
		if db == "" {
			db = s.Table.Qualifier.String()
		}
		if db == "" {
			db = b.currentDb().Name()
		}

		table = b.resolveTable(s.Table.Name.String(), db, asOfLit)
	}
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
			filter := b.buildScalar(inScope, s.ShowTablesOpt.Filter.Filter)
			node = plan.NewFilter(filter, node)
		}
	}

	outScope.node = node
	return
}

func (b *PlanBuilder) buildShowWarnings(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	if s.CountStar {
		unsupportedShow := fmt.Sprintf("SHOW COUNT(*) WARNINGS")
		b.handleErr(sql.ErrUnsupportedFeature.New(unsupportedShow))
	}
	var node sql.Node
	node = plan.ShowWarnings(b.ctx.Session.Warnings())
	if s.Limit != nil {
		if s.Limit.Offset != nil {
			offset := b.buildScalar(inScope, s.Limit.Offset)
			node = plan.NewOffset(offset, node)
		}
		limit := b.buildScalar(inScope, s.Limit.Rowcount)
		node = plan.NewLimit(limit, node)
	}

	outScope.node = node
	return
}

func (b *PlanBuilder) buildShowCollation(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	// show collation statements are functionally identical to selecting from the collations table in
	// information_schema, with slightly different syntax and with some columns aliased.
	// TODO: install information_schema automatically for all catalogs
	infoSchemaSelect, err := Parse(b.ctx, b.cat, "select collation_name as `collation`, character_set_name as charset, id,"+
		"is_default as `default`, is_compiled as compiled, sortlen, pad_attribute from information_schema.collations")
	if err != nil {
		b.handleErr(err)
	}

	if s.ShowCollationFilterOpt != nil {
		filterExpr := b.buildScalar(inScope, *s.ShowCollationFilterOpt)
		// TODO: once collations are properly implemented, we should better be able to handle utf8 -> utf8mb3 comparisons as they're aliases
		filterExpr, _, _ = transform.Expr(filterExpr, func(expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			if exprLiteral, ok := expr.(*expression.Literal); ok {
				const utf8Prefix = "utf8_"
				if strLiteral, ok := exprLiteral.Value().(string); ok && strings.HasPrefix(strLiteral, utf8Prefix) {
					return expression.NewLiteral("utf8mb3_"+strLiteral[len(utf8Prefix):], exprLiteral.Type()), transform.NewTree, nil
				}
			}
			return expr, transform.SameTree, nil
		})
		outScope.node = plan.NewHaving(filterExpr, infoSchemaSelect)
	}

	outScope.node = infoSchemaSelect
	return
}

func (b *PlanBuilder) buildShowEngines(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	infoSchemaSelect, err := Parse(b.ctx, b.cat, "select * from information_schema.engines")
	if err != nil {
		b.handleErr(err)
	}

	outScope.node = infoSchemaSelect
	return
}

func (b *PlanBuilder) buildShowStatus(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	var node sql.Node
	if s.Scope == ast.GlobalStr {
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
			filter = b.buildScalar(inScope, s.Filter.Filter)
		}
	}

	if filter != nil {
		node = plan.NewFilter(filter, node)
	}

	outScope.node = node
	return
}

func (b *PlanBuilder) buildShowCharset(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	var filter sql.Expression

	if s.Filter != nil {
		if s.Filter.Filter != nil {
			filter = b.buildScalar(inScope, s.Filter.Filter)
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
	outScope.node = node
	return
}
