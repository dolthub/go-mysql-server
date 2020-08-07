package parse

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"

	"github.com/opentracing/opentracing-go"
	"gopkg.in/src-d/go-errors.v1"
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/expression/function"
	"github.com/liquidata-inc/go-mysql-server/sql/expression/function/aggregation"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

var (
	// ErrUnsupportedSyntax is thrown when a specific syntax is not already supported
	ErrUnsupportedSyntax = errors.NewKind("unsupported syntax: %s")

	// ErrUnsupportedFeature is thrown when a feature is not already supported
	ErrUnsupportedFeature = errors.NewKind("unsupported feature: %s")

	// ErrInvalidSQLValType is returned when a SQLVal type is not valid.
	ErrInvalidSQLValType = errors.NewKind("invalid SQLVal of type: %d")

	// ErrInvalidSortOrder is returned when a sort order is not valid.
	ErrInvalidSortOrder = errors.NewKind("invalid sort order: %s")

	// errInvalidDescribeFormat is returned when an invalid format string is used for DESCRIBE statements
	errInvalidDescribeFormat = errors.NewKind("invalid format %q for DESCRIBE, supported formats: %s")

	ErrInvalidIndexPrefix = errors.NewKind("invalid index prefix: %v")

	ErrUnknownIndexColumn = errors.NewKind("unknown column: '%s' in %s index '%s'")

	ErrUnknownConstraintDefinition = errors.NewKind("unknown constraint definition: %s, %T")
)

var (
	showVariablesRegex   = regexp.MustCompile(`^show\s+(.*)?variables\s*`)
	showWarningsRegex    = regexp.MustCompile(`^show\s+warnings\s*`)
	fullProcessListRegex = regexp.MustCompile(`^show\s+(full\s+)?processlist$`)
	unlockTablesRegex    = regexp.MustCompile(`^unlock\s+tables$`)
	lockTablesRegex      = regexp.MustCompile(`^lock\s+tables\s`)
	setRegex             = regexp.MustCompile(`^set\s+`)
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
	span, ctx := ctx.Span("parse", opentracing.Tag{Key: "query", Value: query})
	defer span.Finish()

	s := strings.TrimSpace(removeComments(query))
	if strings.HasSuffix(s, ";") {
		s = s[:len(s)-1]
	}

	if s == "" {
		ctx.Warn(0, "query was empty after trimming comments, so it will be ignored")
		return plan.Nothing, nil
	}

	lowerQuery := strings.ToLower(s)

	switch true {
	case showVariablesRegex.MatchString(lowerQuery):
		return parseShowVariables(ctx, s)
	case showWarningsRegex.MatchString(lowerQuery):
		return parseShowWarnings(ctx, s)
	case fullProcessListRegex.MatchString(lowerQuery):
		return plan.NewShowProcessList(), nil
	case unlockTablesRegex.MatchString(lowerQuery):
		return plan.NewUnlockTables(), nil
	case lockTablesRegex.MatchString(lowerQuery):
		return parseLockTables(ctx, s)
	case setRegex.MatchString(lowerQuery):
		s = fixSetQuery(s)
	}

	stmt, err := sqlparser.Parse(s)
	if err != nil {
		return nil, err
	}

	return convert(ctx, stmt, s)
}

func convert(ctx *sql.Context, stmt sqlparser.Statement, query string) (sql.Node, error) {
	if ss, ok := stmt.(sqlparser.SelectStatement); ok {
		return convertSelectStatement(ctx, ss)
	}
	switch n := stmt.(type) {
	default:
		return nil, ErrUnsupportedSyntax.New(sqlparser.String(n))
	case *sqlparser.Show:
		// When a query is empty it means it comes from a subquery, as we don't
		// have the query itself in a subquery. Hence, a SHOW could not be
		// parsed.
		if query == "" {
			return nil, ErrUnsupportedFeature.New("SHOW in subquery")
		}
		return convertShow(ctx, n, query)
	case *sqlparser.Explain:
		return convertExplain(ctx, n)
	case *sqlparser.Insert:
		return convertInsert(ctx, n)
	case *sqlparser.DDL:
		// unlike other statements, DDL statements have loose parsing by default
		ddl, err := sqlparser.ParseStrictDDL(query)
		if err != nil {
			return nil, err
		}
		return convertDDL(ctx, query, ddl.(*sqlparser.DDL))
	case *sqlparser.Set:
		return convertSet(ctx, n)
	case *sqlparser.Use:
		return convertUse(n)
	case *sqlparser.Commit:
		return plan.NewCommit(), nil
	case *sqlparser.Rollback:
		return plan.NewRollback(), nil
	case *sqlparser.Delete:
		return convertDelete(ctx, n)
	case *sqlparser.Update:
		return convertUpdate(ctx, n)
	}
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
		return nil, ErrUnsupportedSyntax.New(sqlparser.String(n))
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
	if n.Scope == sqlparser.GlobalStr {
		return nil, ErrUnsupportedFeature.New("SET global variables")
	}

	var variables = make([]plan.SetVariable, len(n.Exprs))
	for i, e := range n.Exprs {
		expr, err := exprToExpression(ctx, e.Expr)
		if err != nil {
			return nil, err
		}

		name := strings.TrimSpace(e.Name.Lowered())
		expr, err = expression.TransformUp(expr, func(e sql.Expression) (sql.Expression, error) {
			if _, ok := e.(*expression.DefaultColumn); ok {
				return e, nil
			}

			if !e.Resolved() || !sql.IsTextOnly(e.Type()) {
				return e, nil
			}

			txt, err := e.Eval(ctx, nil)
			if err != nil {
				return nil, err
			}

			val, ok := txt.(string)
			if !ok {
				return nil, ErrUnsupportedFeature.New("invalid qualifiers in set variable names")
			}

			switch strings.ToLower(val) {
			case sqlparser.KeywordString(sqlparser.ON):
				return expression.NewLiteral(int64(1), sql.Int64), nil
			case sqlparser.KeywordString(sqlparser.TRUE):
				return expression.NewLiteral(true, sql.Boolean), nil
			case sqlparser.KeywordString(sqlparser.OFF):
				return expression.NewLiteral(int64(0), sql.Int64), nil
			case sqlparser.KeywordString(sqlparser.FALSE):
				return expression.NewLiteral(false, sql.Boolean), nil
			}

			return e, nil
		})

		if err != nil {
			return nil, err
		}

		// special case: for system variables, MySQL allows naked strings (without quotes), which get interpreted as
		// unresolved columns.
		if uc, ok := expr.(*expression.UnresolvedColumn); ok && uc.Table() == "" {
			expr = expression.NewLiteral(uc.Name(), sql.LongText)
		}

		variables[i] = plan.SetVariable{
			Name:  name,
			Value: expr,
		}
	}

	return plan.NewSet(variables...), nil
}

func convertShow(ctx *sql.Context, s *sqlparser.Show, query string) (sql.Node, error) {
	showType := strings.ToLower(s.Type)
	switch showType {
	case "create table", "create view":
		return plan.NewShowCreateTable(
			plan.NewUnresolvedTable(s.Table.Name.String(), s.Table.Qualifier.String()),
			showType == "create view",
		), nil
	case "create database", "create schema":
		return plan.NewShowCreateDatabase(
			sql.UnresolvedDatabase(s.Database),
			s.IfNotExists,
		), nil
	case "index":
		return plan.NewShowIndexes(plan.NewUnresolvedTable(s.Table.Name.String(), s.Database)), nil
	case sqlparser.KeywordString(sqlparser.TABLES):
		var dbName string
		var filter sql.Expression
		var full bool
		if s.ShowTablesOpt != nil {
			dbName = s.ShowTablesOpt.DbName
			full = s.ShowTablesOpt.Full != ""

			if s.ShowTablesOpt.Filter != nil {
				if s.ShowTablesOpt.Filter.Filter != nil {
					var err error
					filter, err = exprToExpression(ctx, s.ShowTablesOpt.Filter.Filter)
					if err != nil {
						return nil, err
					}
				} else if s.ShowTablesOpt.Filter.Like != "" {
					filter = expression.NewLike(
						expression.NewUnresolvedColumn("Table"),
						expression.NewLiteral(s.ShowTablesOpt.Filter.Like, sql.LongText),
					)
				}
			}
		}

		var node sql.Node = plan.NewShowTables(sql.UnresolvedDatabase(dbName), full)
		if filter != nil {
			node = plan.NewFilter(filter, node)
		}

		return node, nil
	case sqlparser.KeywordString(sqlparser.DATABASES), sqlparser.KeywordString(sqlparser.SCHEMAS):
		return plan.NewShowDatabases(), nil
	case sqlparser.KeywordString(sqlparser.FIELDS), sqlparser.KeywordString(sqlparser.COLUMNS):
		// TODO(erizocosmico): vitess parser does not support EXTENDED.
		table := plan.NewUnresolvedTable(s.OnTable.Name.String(), s.OnTable.Qualifier.String())
		full := s.ShowTablesOpt != nil && s.ShowTablesOpt.Full != ""

		var node sql.Node = plan.NewShowColumns(full, table)

		if s.ShowTablesOpt != nil && s.ShowTablesOpt.Filter != nil {
			if s.ShowTablesOpt.Filter.Like != "" {
				pattern := expression.NewLiteral(s.ShowTablesOpt.Filter.Like, sql.LongText)

				node = plan.NewFilter(
					expression.NewLike(
						expression.NewUnresolvedColumn("Field"),
						pattern,
					),
					node,
				)
			}

			if s.ShowTablesOpt.Filter.Filter != nil {
				filter, err := exprToExpression(ctx, s.ShowTablesOpt.Filter.Filter)
				if err != nil {
					return nil, err
				}

				node = plan.NewFilter(filter, node)
			}
		}

		return node, nil
	case sqlparser.KeywordString(sqlparser.TABLE):
		return parseShowTableStatus(ctx, query)
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
			filterExpr, err := exprToExpression(ctx, *s.ShowCollationFilterOpt)
			if err != nil {
				return nil, err
			}
			return plan.NewFilter(filterExpr, infoSchemaSelect), nil
		}

		return infoSchemaSelect, nil
	default:
		unsupportedShow := fmt.Sprintf("SHOW %s", s.Type)
		return nil, ErrUnsupportedFeature.New(unsupportedShow)
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
	if u.Type == sqlparser.UnionStr || u.Type == sqlparser.UnionAllStr {
		return plan.NewUnion(left, right), nil
	} else if u.Type == sqlparser.UnionDistinctStr {
		return plan.NewDistinct(plan.NewUnion(left, right)), nil
	}
	return nil, ErrUnsupportedFeature.New(u.Type)
}

func convertSelect(ctx *sql.Context, s *sqlparser.Select) (sql.Node, error) {
	node, err := tableExprsToTable(ctx, s.From)
	if err != nil {
		return nil, err
	}

	if s.Where != nil {
		node, err = whereToFilter(ctx, s.Where, node)
		if err != nil {
			return nil, err
		}
	}

	node, err = selectToProjectOrGroupBy(ctx, s.SelectExprs, s.GroupBy, node)
	if err != nil {
		return nil, err
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
	} else if ok, val := sql.HasDefaultValue(ctx.Session, "sql_select_limit"); !ok {
		limit := mustCastNumToInt64(val)
		node = plan.NewLimit(limit, node)
	}

	return node, nil
}

func convertDDL(ctx *sql.Context, query string, c *sqlparser.DDL) (sql.Node, error) {
	switch strings.ToLower(c.Action) {
	case sqlparser.CreateStr:
		if !c.View.IsEmpty() {
			return convertCreateView(ctx, query, c)
		}
		return convertCreateTable(ctx, c)
	case sqlparser.DropStr:
		if len(c.FromViews) != 0 {
			return convertDropView(ctx, c)
		}
		return convertDropTable(ctx, c)
	case sqlparser.AlterStr:
		return convertAlterTable(ctx, c)
	case sqlparser.RenameStr:
		return convertRenameTable(ctx, c)
	default:
		return nil, ErrUnsupportedSyntax.New(sqlparser.String(c))
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
	//TODO: support multiple constraints in a single ALTER statement
	if ddl.ConstraintAction != "" && len(ddl.TableSpec.Constraints) == 1 {
		table := plan.NewUnresolvedTable(ddl.Table.Name.String(), ddl.Table.Qualifier.String())
		parsedConstraint, err := convertConstraintDefinition(ctx, ddl.TableSpec.Constraints[0])
		if err != nil {
			return nil, err
		}
		switch strings.ToLower(ddl.ConstraintAction) {
		case sqlparser.AddStr:
			if fkConstraint, ok := parsedConstraint.(*sql.ForeignKeyConstraint); ok {
				return plan.NewAlterAddForeignKey(
					table,
					plan.NewUnresolvedTable(fkConstraint.ReferencedTable, ddl.Table.Qualifier.String()),
					fkConstraint), nil
			} else {
				return nil, ErrUnsupportedFeature.New(sqlparser.String(ddl))
			}
		case sqlparser.DropStr:
			switch c := parsedConstraint.(type) {
			case *sql.ForeignKeyConstraint:
				return plan.NewAlterDropForeignKey(table, c), nil
			case namedConstraint:
				// For simple named constraint drops, fill in a partial foreign key constraint. This will need to be changed if
				// we ever support other kinds of constraints than foreign keys (e.g. CHECK)
				return plan.NewAlterDropForeignKey(table, &sql.ForeignKeyConstraint{
					Name: c.name,
				}), nil
			default:
				return nil, ErrUnsupportedFeature.New(sqlparser.String(ddl))
			}
		}
	}
	switch strings.ToLower(ddl.ColumnAction) {
	case sqlparser.AddStr:
		sch, err := TableSpecToSchema(ctx, ddl.TableSpec)
		if err != nil {
			return nil, err
		}
		return plan.NewAddColumn(sql.UnresolvedDatabase(""), ddl.Table.Name.String(), sch[0], columnOrderToColumnOrder(ddl.ColumnOrder)), nil
	case sqlparser.DropStr:
		return plan.NewDropColumn(sql.UnresolvedDatabase(""), ddl.Table.Name.String(), ddl.Column.String()), nil
	case sqlparser.RenameStr:
		return plan.NewRenameColumn(sql.UnresolvedDatabase(""), ddl.Table.Name.String(), ddl.Column.String(), ddl.ToColumn.String()), nil
	case sqlparser.ModifyStr, sqlparser.ChangeStr:
		sch, err := TableSpecToSchema(nil, ddl.TableSpec)
		if err != nil {
			return nil, err
		}
		return plan.NewModifyColumn(sql.UnresolvedDatabase(""), ddl.Table.Name.String(), ddl.Column.String(), sch[0], columnOrderToColumnOrder(ddl.ColumnOrder)), nil
	default:
		return nil, ErrUnsupportedFeature.New(sqlparser.String(ddl))
	}
}

func convertAlterIndex(ctx *sql.Context, ddl *sqlparser.DDL) (sql.Node, error) {
	table := plan.NewUnresolvedTable(ddl.Table.Name.String(), ddl.Table.Qualifier.String())
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
			constraint = sql.IndexConstraint_Fulltext
		case sqlparser.SpatialStr:
			constraint = sql.IndexConstraint_Spatial
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
						return nil, ErrInvalidIndexPrefix.New(length)
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
			if option.Name == sqlparser.KeywordString(sqlparser.COMMENT_KEYWORD) {
				comment = string(option.Value.Val)
			}
		}

		return plan.NewAlterCreateIndex(table, ddl.IndexSpec.ToName.String(), using, constraint, columns, comment), nil
	case sqlparser.DropStr:
		return plan.NewAlterDropIndex(table, ddl.IndexSpec.ToName.String()), nil
	case sqlparser.RenameStr:
		return plan.NewAlterRenameIndex(table, ddl.IndexSpec.FromName.String(), ddl.IndexSpec.ToName.String()), nil
	default:
		return nil, ErrUnsupportedFeature.New(sqlparser.String(ddl))
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
		plan.NewUnresolvedTable(ddl.Table.Name.String(), ddl.Table.Qualifier.String()),
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
	tableNames := make([]string, len(c.FromTables))
	for i, t := range c.FromTables {
		tableNames[i] = t.Name.String()
	}
	return plan.NewDropTable(sql.UnresolvedDatabase(""), c.IfExists, tableNames...), nil
}

func convertCreateTable(ctx *sql.Context, c *sqlparser.DDL) (sql.Node, error) {
	schema, err := TableSpecToSchema(nil, c.TableSpec)
	if err != nil {
		return nil, err
	}

	var fkDefs []*sql.ForeignKeyConstraint
	for _, unknownConstraint := range c.TableSpec.Constraints {
		parsedConstraint, err := convertConstraintDefinition(ctx, unknownConstraint)
		if err != nil {
			return nil, err
		}
		switch constraint := parsedConstraint.(type) {
		case *sql.ForeignKeyConstraint:
			fkDefs = append(fkDefs, constraint)
		default:
			return nil, ErrUnknownConstraintDefinition.New(unknownConstraint.Name, unknownConstraint)
		}
	}

	var idxDefs []*plan.IndexDefinition
	for _, idxDef := range c.TableSpec.Indexes {
		if idxDef.Info.Primary {
			continue
		}

		//TODO: add vitess support for FULLTEXT
		constraint := sql.IndexConstraint_None
		if idxDef.Info.Unique {
			constraint = sql.IndexConstraint_Unique
		} else if idxDef.Info.Spatial {
			constraint = sql.IndexConstraint_Spatial
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
						return nil, ErrInvalidIndexPrefix.New(length)
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

	return plan.NewCreateTable(
		sql.UnresolvedDatabase(""), c.Table.Name.String(), schema, c.IfNotExists, idxDefs, fkDefs), nil
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
		return &sql.ForeignKeyConstraint{
			Name:              cd.Name,
			Columns:           columns,
			ReferencedTable:   fkConstraint.ReferencedTable.Name.String(),
			ReferencedColumns: refColumns,
			OnUpdate:          convertReferenceAction(fkConstraint.OnUpdate),
			OnDelete:          convertReferenceAction(fkConstraint.OnDelete),
		}, nil
	} else if len(cd.Name) > 0 && cd.Details == nil {
		return namedConstraint{cd.Name}, nil
	}
	return nil, ErrUnknownConstraintDefinition.New(cd.Name, cd)
}

func convertReferenceAction(action sqlparser.ReferenceAction) sql.ForeignKeyReferenceOption {
	switch action {
	case sqlparser.Restrict:
		return sql.ForeignKeyReferenceOption_Restrict
	case sqlparser.Cascade:
		return sql.ForeignKeyReferenceOption_Cascade
	case sqlparser.NoAction:
		return sql.ForeignKeyReferenceOption_NoAction
	case sqlparser.SetNull:
		return sql.ForeignKeyReferenceOption_SetNull
	case sqlparser.SetDefault:
		return sql.ForeignKeyReferenceOption_SetDefault
	default:
		return sql.ForeignKeyReferenceOption_DefaultAction
	}
}

func convertCreateView(ctx *sql.Context, query string, c *sqlparser.DDL) (sql.Node, error) {
	selectStatement, ok := c.ViewExpr.(*sqlparser.Select)
	if !ok {
		return nil, ErrUnsupportedSyntax.New(sqlparser.String(c.ViewExpr))
	}

	queryNode, err := convertSelect(ctx, selectStatement)
	if err != nil {
		return nil, err
	}

	selectStr := query[c.ViewSelectPositionStart:c.ViewSelectPositionEnd]
	queryAlias := plan.NewSubqueryAlias(c.View.Name.String(), selectStr, queryNode)

	return plan.NewCreateView(
		sql.UnresolvedDatabase(""), c.View.Name.String(), []string{}, queryAlias, c.OrReplace), nil
}

func convertDropView(ctx *sql.Context, c *sqlparser.DDL) (sql.Node, error) {
	plans := make([]sql.Node, len(c.FromViews))
	for i, v := range c.FromViews {
		plans[i] = plan.NewSingleDropView(sql.UnresolvedDatabase(""), v.Name.String())
	}
	return plan.NewDropView(plans, c.IfExists), nil
}

func convertInsert(ctx *sql.Context, i *sqlparser.Insert) (sql.Node, error) {
	if len(i.OnDup) > 0 {
		return nil, ErrUnsupportedFeature.New("ON DUPLICATE KEY")
	}

	if len(i.Ignore) > 0 {
		return nil, ErrUnsupportedSyntax.New(sqlparser.String(i))
	}

	isReplace := i.Action == sqlparser.ReplaceStr

	src, err := insertRowsToNode(ctx, i.Rows)
	if err != nil {
		return nil, err
	}

	return plan.NewInsertInto(
		plan.NewUnresolvedTable(i.Table.Name.String(), i.Table.Qualifier.String()),
		src,
		isReplace,
		columnsToStrings(i.Columns),
	), nil
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

	updateExprs, err := updateExprsToExpressions(ctx, d.Exprs)
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

// TableSpecToSchema creates a sql.Schema from a parsed TableSpec
func TableSpecToSchema(ctx *sql.Context, tableSpec *sqlparser.TableSpec) (sql.Schema, error) {
	err := validateIndexes(tableSpec)

	if err != nil {
		return nil, err
	}

	var schema sql.Schema
	for _, cd := range tableSpec.Columns {
		column, err := columnDefinitionToColumn(ctx, cd, tableSpec.Indexes)
		if err != nil {
			return nil, err
		}

		schema = append(schema, column)
	}

	return schema, nil
}

func validateIndexes(tableSpec *sqlparser.TableSpec) error {
	lwrNames := make(map[string]bool)
	for _, col := range tableSpec.Columns {
		lwrNames[col.Name.Lowered()] = true
	}

	for _, idx := range tableSpec.Indexes {
		for _, col := range idx.Columns {
			if !lwrNames[col.Column.Lowered()] {
				return ErrUnknownIndexColumn.New(col.Column.String(), idx.Info.Type, idx.Info.Name.String())
			}
		}
	}

	return nil
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

	var defaultVal interface{}
	if cd.Type.Default != nil {
		dflt, err := exprToExpression(ctx, cd.Type.Default)
		if err != nil {
			return nil, err
		}

		// TODO: this isn't quite right -- some default expressions here (like function calls) need to be stored by the
		//  implementor and deferred until row insertion time. We can't do that, but we can at least do a better job
		//  detecting when this happens and erroring out.
		defaultVal, err = dflt.Eval(ctx, nil)
		if err != nil {
			return nil, ErrUnsupportedFeature.New("column defaults must be evaluable at schema modification time")
		}
	}

	return &sql.Column{
		Nullable:   !isPkey && !bool(cd.Type.NotNull),
		Type:       internalTyp,
		Name:       cd.Name.String(),
		PrimaryKey: isPkey,
		Default:    defaultVal,
		Comment:    comment,
	}, nil
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
	case *sqlparser.Select:
		return convertSelect(ctx, v)
	case *sqlparser.Union:
		return nil, ErrUnsupportedFeature.New("UNION")
	case sqlparser.Values:
		return valuesToValues(ctx, v)
	default:
		return nil, ErrUnsupportedSyntax.New(sqlparser.String(ir))
	}
}

func valuesToValues(ctx *sql.Context, v sqlparser.Values) (sql.Node, error) {
	exprTuples := make([][]sql.Expression, len(v))
	for i, vt := range v {
		exprs := make([]sql.Expression, len(vt))
		exprTuples[i] = exprs
		for j, e := range vt {
			expr, err := exprToExpression(ctx, e)
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
		return nil, ErrUnsupportedFeature.New("zero tables in FROM")
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
		return nil, ErrUnsupportedSyntax.New(sqlparser.String(te))
	case *sqlparser.AliasedTableExpr:
		// TODO: Add support for qualifier.
		switch e := t.Expr.(type) {
		case sqlparser.TableName:
			var node *plan.UnresolvedTable
			if t.AsOf != nil {
				asOfExpr, err := exprToExpression(ctx, t.AsOf.Time)
				if err != nil {
					return nil, err
				}
				node = plan.NewUnresolvedTableAsOf(e.Name.String(), e.Qualifier.String(), asOfExpr)
			} else {
				node = plan.NewUnresolvedTable(e.Name.String(), e.Qualifier.String())
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
				return nil, ErrUnsupportedFeature.New("subquery without alias")
			}

			return plan.NewSubqueryAlias(t.As.String(), sqlparser.String(e.Select), node), nil
		default:
			return nil, ErrUnsupportedSyntax.New(sqlparser.String(te))
		}
	case *sqlparser.JoinTableExpr:
		// TODO: add support for using, once we have proper table
		// qualification of fields
		if len(t.Condition.Using) > 0 {
			return nil, ErrUnsupportedFeature.New("USING clause on join")
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

		cond, err := exprToExpression(ctx, t.Condition.On)
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
			return nil, ErrUnsupportedFeature.New("Join type " + t.Join)
		}
	}
}

func whereToFilter(ctx *sql.Context, w *sqlparser.Where, child sql.Node) (*plan.Filter, error) {
	c, err := exprToExpression(ctx, w.Expr)
	if err != nil {
		return nil, err
	}

	return plan.NewFilter(c, child), nil
}

func orderByToSort(ctx *sql.Context, ob sqlparser.OrderBy, child sql.Node) (*plan.Sort, error) {
	var sortFields []plan.SortField
	for _, o := range ob {
		e, err := exprToExpression(ctx, o.Expr)
		if err != nil {
			return nil, err
		}

		var so plan.SortOrder
		switch strings.ToLower(o.Direction) {
		default:
			return nil, ErrInvalidSortOrder.New(o.Direction)
		case sqlparser.AscScr:
			so = plan.Ascending
		case sqlparser.DescScr:
			so = plan.Descending
		}

		sf := plan.SortField{Column: e, Order: so}
		sortFields = append(sortFields, sf)
	}

	return plan.NewSort(sortFields, child), nil
}

func limitToLimit(
	ctx *sql.Context,
	limit sqlparser.Expr,
	child sql.Node,
) (*plan.Limit, error) {
	rowCount, err := getInt64Value(ctx, limit, "LIMIT with non-integer literal")
	if err != nil {
		return nil, err
	}

	if rowCount < 0 {
		return nil, ErrUnsupportedSyntax.New("LIMIT must be >= 0")
	}

	return plan.NewLimit(rowCount, child), nil
}

func havingToHaving(ctx *sql.Context, having *sqlparser.Where, node sql.Node) (sql.Node, error) {
	cond, err := exprToExpression(ctx, having.Expr)
	if err != nil {
		return nil, err
	}

	return plan.NewHaving(cond, node), nil
}

func offsetToOffset(
	ctx *sql.Context,
	offset sqlparser.Expr,
	child sql.Node,
) (*plan.Offset, error) {
	o, err := getInt64Value(ctx, offset, "OFFSET with non-integer literal")
	if err != nil {
		return nil, err
	}

	if o < 0 {
		return nil, ErrUnsupportedSyntax.New("OFFSET must be >= 0")
	}

	return plan.NewOffset(o, child), nil
}

// getInt64Literal returns an int64 *expression.Literal for the value given, or an unsupported error with the string
// given if the expression doesn't represent an integer literal.
func getInt64Literal(ctx *sql.Context, expr sqlparser.Expr, errStr string) (*expression.Literal, error) {
	e, err := exprToExpression(ctx, expr)
	if err != nil {
		return nil, err
	}

	nl, ok := e.(*expression.Literal)
	if !ok || !sql.IsInteger(nl.Type()) {
		return nil, ErrUnsupportedFeature.New(errStr)
	} else {
		i64, err := sql.Int64.Convert(nl.Value())
		if err != nil {
			return nil, ErrUnsupportedFeature.New(errStr)
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

func isAggregate(e sql.Expression) bool {
	var isAgg bool
	sql.Inspect(e, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.UnresolvedFunction:
			isAgg = isAgg || e.IsAggregate
		case *aggregation.CountDistinct:
			isAgg = true
		}

		return true
	})
	return isAgg
}

func selectToProjectOrGroupBy(
	ctx *sql.Context,
	se sqlparser.SelectExprs,
	g sqlparser.GroupBy,
	child sql.Node,
) (sql.Node, error) {
	selectExprs, err := selectExprsToExpressions(ctx, se)
	if err != nil {
		return nil, err
	}

	isAgg := len(g) > 0
	if !isAgg {
		for _, e := range selectExprs {
			if isAggregate(e) {
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

func exprToExpression(ctx *sql.Context, e sqlparser.Expr) (sql.Expression, error) {
	switch v := e.(type) {
	default:
		return nil, ErrUnsupportedSyntax.New(sqlparser.String(e))
	case *sqlparser.Default:
		return expression.NewDefaultColumn(v.ColName), nil
	case *sqlparser.SubstrExpr:
		var (
			name sql.Expression
			err  error
		)
		if v.Name != nil {
			name, err = exprToExpression(ctx, v.Name)
		} else {
			name, err = exprToExpression(ctx, v.StrVal)
		}
		if err != nil {
			return nil, err
		}
		from, err := exprToExpression(ctx, v.From)
		if err != nil {
			return nil, err
		}

		if v.To == nil {
			return function.NewSubstring(name, from)
		}
		to, err := exprToExpression(ctx, v.To)
		if err != nil {
			return nil, err
		}
		return function.NewSubstring(name, from, to)
	case *sqlparser.ComparisonExpr:
		return comparisonExprToExpression(ctx, v)
	case *sqlparser.IsExpr:
		return isExprToExpression(ctx, v)
	case *sqlparser.NotExpr:
		c, err := exprToExpression(ctx, v.Expr)
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

		if v.Distinct {
			if v.Name.Lowered() != "count" {
				return nil, ErrUnsupportedSyntax.New("DISTINCT on non-COUNT aggregations")
			}

			if len(exprs) != 1 {
				return nil, ErrUnsupportedSyntax.New("more than one expression in COUNT")
			}

			return aggregation.NewCountDistinct(exprs[0]), nil
		}

		return expression.NewUnresolvedFunction(v.Name.Lowered(),
			isAggregateFunc(v), exprs...), nil
	case *sqlparser.ParenExpr:
		return exprToExpression(ctx, v.Expr)
	case *sqlparser.AndExpr:
		lhs, err := exprToExpression(ctx, v.Left)
		if err != nil {
			return nil, err
		}

		rhs, err := exprToExpression(ctx, v.Right)
		if err != nil {
			return nil, err
		}

		return expression.NewAnd(lhs, rhs), nil
	case *sqlparser.OrExpr:
		lhs, err := exprToExpression(ctx, v.Left)
		if err != nil {
			return nil, err
		}

		rhs, err := exprToExpression(ctx, v.Right)
		if err != nil {
			return nil, err
		}

		return expression.NewOr(lhs, rhs), nil
	case *sqlparser.ConvertExpr:
		expr, err := exprToExpression(ctx, v.Expr)
		if err != nil {
			return nil, err
		}

		return expression.NewConvert(expr, v.Type.Type), nil
	case *sqlparser.RangeCond:
		val, err := exprToExpression(ctx, v.Left)
		if err != nil {
			return nil, err
		}

		lower, err := exprToExpression(ctx, v.From)
		if err != nil {
			return nil, err
		}

		upper, err := exprToExpression(ctx, v.To)
		if err != nil {
			return nil, err
		}

		switch strings.ToLower(v.Operator) {
		case sqlparser.BetweenStr:
			return expression.NewBetween(val, lower, upper), nil
		case sqlparser.NotBetweenStr:
			return expression.NewNot(expression.NewBetween(val, lower, upper)), nil
		default:
			return nil, ErrUnsupportedFeature.New(fmt.Sprintf("RangeCond with operator: %s", v.Operator))
		}
	case sqlparser.ValTuple:
		var exprs = make([]sql.Expression, len(v))
		for i, e := range v {
			expr, err := exprToExpression(ctx, e)
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
		return exprToExpression(ctx, v.Expr)
	}
}

func isAggregateFunc(v *sqlparser.FuncExpr) bool {
	switch v.Name.Lowered() {
	case "first", "last":
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

		return convertInt(v, 16)
	case sqlparser.HexVal:
		val, err := v.HexDecode()
		if err != nil {
			return nil, err
		}
		return expression.NewLiteral(val, sql.LongBlob), nil
	case sqlparser.ValArg:
		return expression.NewLiteral(string(v.Val), sql.LongText), nil
	case sqlparser.BitVal:
		return expression.NewLiteral(v.Val[0] == '1', sql.Boolean), nil
	}

	return nil, ErrInvalidSQLValType.New(v.Type)
}

func isExprToExpression(ctx *sql.Context, c *sqlparser.IsExpr) (sql.Expression, error) {
	e, err := exprToExpression(ctx, c.Expr)
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
		return nil, ErrUnsupportedSyntax.New(sqlparser.String(c))
	}
}

func comparisonExprToExpression(ctx *sql.Context, c *sqlparser.ComparisonExpr) (sql.Expression, error) {
	left, err := exprToExpression(ctx, c.Left)
	if err != nil {
		return nil, err
	}

	right, err := exprToExpression(ctx, c.Right)
	if err != nil {
		return nil, err
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
			return nil, ErrUnsupportedFeature.New(fmt.Sprintf("IN %T", right))
		}
	case sqlparser.NotInStr:
		switch right.(type) {
		case expression.Tuple:
			return expression.NewNotInTuple(left, right), nil
		case *plan.Subquery:
			return plan.NewNotInSubquery(left, right), nil
		default:
			return nil, ErrUnsupportedFeature.New(fmt.Sprintf("NOT IN %T", right))
		}
	case sqlparser.LikeStr:
		return expression.NewLike(left, right), nil
	case sqlparser.NotLikeStr:
		return expression.NewNot(expression.NewLike(left, right)), nil
	default:
		return nil, ErrUnsupportedFeature.New(c.Operator)
	}
}

func groupByToExpressions(ctx *sql.Context, g sqlparser.GroupBy) ([]sql.Expression, error) {
	es := make([]sql.Expression, len(g))
	for i, ve := range g {
		e, err := exprToExpression(ctx, ve)
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
		return nil, ErrUnsupportedSyntax.New(sqlparser.String(e))
	case *sqlparser.StarExpr:
		if e.TableName.IsEmpty() {
			return expression.NewStar(), nil
		}
		return expression.NewQualifiedStar(e.TableName.Name.String()), nil
	case *sqlparser.AliasedExpr:
		expr, err := exprToExpression(ctx, e.Expr)
		if err != nil {
			return nil, err
		}

		if e.As.String() == "" {
			return expr, nil
		}

		return expression.NewAlias(e.As.String(), expr), nil
	}
}

func unaryExprToExpression(ctx *sql.Context, e *sqlparser.UnaryExpr) (sql.Expression, error) {
	switch strings.ToLower(e.Operator) {
	case sqlparser.MinusStr:
		expr, err := exprToExpression(ctx, e.Expr)
		if err != nil {
			return nil, err
		}

		return expression.NewUnaryMinus(expr), nil
	case sqlparser.PlusStr:
		// Unary plus expressions do nothing (do not turn the expression positive). Just return the underlying expression.
		return exprToExpression(ctx, e.Expr)

	default:
		return nil, ErrUnsupportedFeature.New("unary operator: " + e.Operator)
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

		l, err := exprToExpression(ctx, be.Left)
		if err != nil {
			return nil, err
		}

		r, err := exprToExpression(ctx, be.Right)
		if err != nil {
			return nil, err
		}

		_, lok := l.(*expression.Interval)
		_, rok := r.(*expression.Interval)
		if lok && be.Operator == "-" {
			return nil, ErrUnsupportedSyntax.New("subtracting from an interval")
		} else if (lok || rok) && be.Operator != "+" && be.Operator != "-" {
			return nil, ErrUnsupportedSyntax.New("only + and - can be used to add or subtract intervals from dates")
		} else if lok && rok {
			return nil, ErrUnsupportedSyntax.New("intervals cannot be added or subtracted from other intervals")
		}

		return expression.NewArithmetic(l, r, be.Operator), nil

	default:
		return nil, ErrUnsupportedFeature.New(be.Operator)
	}
}

func caseExprToExpression(ctx *sql.Context, e *sqlparser.CaseExpr) (sql.Expression, error) {
	var expr sql.Expression
	var err error

	if e.Expr != nil {
		expr, err = exprToExpression(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
	}

	var branches []expression.CaseBranch
	for _, w := range e.Whens {
		var cond sql.Expression
		cond, err = exprToExpression(ctx, w.Cond)
		if err != nil {
			return nil, err
		}

		var val sql.Expression
		val, err = exprToExpression(ctx, w.Val)
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
		elseExpr, err = exprToExpression(ctx, e.Else)
		if err != nil {
			return nil, err
		}
	}

	return expression.NewCase(expr, branches, elseExpr), nil
}

func intervalExprToExpression(ctx *sql.Context, e *sqlparser.IntervalExpr) (sql.Expression, error) {
	expr, err := exprToExpression(ctx, e.Expr)
	if err != nil {
		return nil, err
	}

	return expression.NewInterval(expr, e.Unit), nil
}

func updateExprsToExpressions(ctx *sql.Context, e sqlparser.UpdateExprs) ([]sql.Expression, error) {
	res := make([]sql.Expression, len(e))
	for i, updateExpr := range e {
		colName, err := exprToExpression(ctx, updateExpr.Name)
		if err != nil {
			return nil, err
		}
		innerExpr, err := exprToExpression(ctx, updateExpr.Expr)
		if err != nil {
			return nil, err
		}
		res[i] = expression.NewSetField(colName, innerExpr)
	}
	return res, nil
}

func removeComments(s string) string {
	r := bufio.NewReader(strings.NewReader(s))
	var result []rune
	for {
		ru, _, err := r.ReadRune()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		switch ru {
		case '\'', '"':
			result = append(result, ru)
			result = append(result, readString(r, ru == '\'')...)
		case '-':
			peeked, err := r.Peek(2)
			if err == nil &&
				len(peeked) == 2 &&
				rune(peeked[0]) == '-' &&
				rune(peeked[1]) == ' ' {
				discardUntilEOL(r)
			} else {
				result = append(result, ru)
			}
		case '/':
			peeked, err := r.Peek(1)
			if err == nil &&
				len(peeked) == 1 &&
				rune(peeked[0]) == '*' {
				// read the char we peeked
				_, _, _ = r.ReadRune()
				discardMultilineComment(r)
			} else {
				result = append(result, ru)
			}
		default:
			result = append(result, ru)
		}
	}
	return string(result)
}
func discardUntilEOL(r *bufio.Reader) {
	for {
		ru, _, err := r.ReadRune()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		if ru == '\n' {
			break
		}
	}
}
func discardMultilineComment(r *bufio.Reader) {
	for {
		ru, _, err := r.ReadRune()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		if ru == '*' {
			peeked, err := r.Peek(1)
			if err == nil && len(peeked) == 1 && rune(peeked[0]) == '/' {
				// read the rune we just peeked
				_, _, _ = r.ReadRune()
				break
			}
		}
	}
}
func readString(r *bufio.Reader, single bool) []rune {
	var result []rune
	var escaped bool
	for {
		ru, _, err := r.ReadRune()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		result = append(result, ru)
		if (!single && ru == '"' && !escaped) ||
			(single && ru == '\'' && !escaped) {
			break
		}
		escaped = false
		if ru == '\\' {
			escaped = true
		}
	}
	return result
}

func parseShowTableStatus(ctx *sql.Context, query string) (sql.Node, error) {
	buf := bufio.NewReader(strings.NewReader(query))
	err := parseFuncs{
		expect("show"),
		skipSpaces,
		expect("table"),
		skipSpaces,
		expect("status"),
		skipSpaces,
	}.exec(buf)

	if err != nil {
		return nil, err
	}

	if _, err = buf.Peek(1); err == io.EOF {
		return plan.NewShowTableStatus(), nil
	}

	var clause string
	if err := readIdent(&clause)(buf); err != nil {
		return nil, err
	}

	if err := skipSpaces(buf); err != nil {
		return nil, err
	}

	switch strings.ToLower(clause) {
	case "from", "in":
		var db string
		if err := readQuotableIdent(&db)(buf); err != nil {
			return nil, err
		}

		return plan.NewShowTableStatus(db), nil
	case "where", "like":
		bs, err := ioutil.ReadAll(buf)
		if err != nil {
			return nil, err
		}

		expr, err := parseExpr(ctx, string(bs))
		if err != nil {
			return nil, err
		}

		var filter sql.Expression
		if strings.ToLower(clause) == "like" {
			filter = expression.NewLike(
				expression.NewUnresolvedColumn("Name"),
				expr,
			)
		} else {
			filter = expr
		}

		return plan.NewFilter(
			filter,
			plan.NewShowTableStatus(),
		), nil
	default:
		return nil, errUnexpectedSyntax.New("one of: FROM, IN, LIKE or WHERE", clause)
	}
}

var fixSessionRegex = regexp.MustCompile(`(,\s*|(set|SET)\s+)(SESSION|session)\s+([a-zA-Z0-9_]+)\s*=`)
var fixGlobalRegex = regexp.MustCompile(`(,\s*|(set|SET)\s+)(GLOBAL|global)\s+([a-zA-Z0-9_]+)\s*=`)

func fixSetQuery(s string) string {
	s = fixSessionRegex.ReplaceAllString(s, `$1@@session.$4 =`)
	s = fixGlobalRegex.ReplaceAllString(s, `$1@@global.$4 =`)
	return s
}
