package optbuilder

import (
	"encoding/hex"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"gopkg.in/src-d/go-errors.v1"
	"regexp"
	"strconv"
	"strings"
)

var (
	errInvalidDescribeFormat = errors.NewKind("invalid format %q for DESCRIBE, supported formats: %s")

	errInvalidSortOrder = errors.NewKind("invalid sort order: %s")

	ErrPrimaryKeyOnNullField = errors.NewKind("All parts of PRIMARY KEY must be NOT NULL")

	tableCharsetOptionRegex = regexp.MustCompile(`(?i)(DEFAULT)?\s+CHARACTER\s+SET((\s*=?\s*)|\s+)([A-Za-z0-9_]+)`)

	tableCollationOptionRegex = regexp.MustCompile(`(?i)(DEFAULT)?\s+COLLATE((\s*=?\s*)|\s+)([A-Za-z0-9_]+)`)
)

func columnsToStrings(cols sqlparser.Columns) []string {
	if len(cols) == 0 {
		return nil
	}
	res := make([]string, len(cols))
	for i, c := range cols {
		res[i] = c.String()
	}

	return res
}

// Convert an integer, represented by the specified string in the specified
// base, to its smallest representation possible, out of:
// int8, uint8, int16, uint16, int32, uint32, int64 and uint64
func (b *PlanBuilder) convertInt(value string, base int) *expression.Literal {
	if i8, err := strconv.ParseInt(value, base, 8); err == nil {
		return expression.NewLiteral(int8(i8), types.Int8)
	}
	if ui8, err := strconv.ParseUint(value, base, 8); err == nil {
		return expression.NewLiteral(uint8(ui8), types.Uint8)
	}
	if i16, err := strconv.ParseInt(value, base, 16); err == nil {
		return expression.NewLiteral(int16(i16), types.Int16)
	}
	if ui16, err := strconv.ParseUint(value, base, 16); err == nil {
		return expression.NewLiteral(uint16(ui16), types.Uint16)
	}
	if i32, err := strconv.ParseInt(value, base, 32); err == nil {
		return expression.NewLiteral(int32(i32), types.Int32)
	}
	if ui32, err := strconv.ParseUint(value, base, 32); err == nil {
		return expression.NewLiteral(uint32(ui32), types.Uint32)
	}
	if i64, err := strconv.ParseInt(value, base, 64); err == nil {
		return expression.NewLiteral(int64(i64), types.Int64)
	}
	if ui64, err := strconv.ParseUint(value, base, 64); err == nil {
		return expression.NewLiteral(uint64(ui64), types.Uint64)
	}
	if decimal, _, err := types.InternalDecimalType.Convert(value); err == nil {
		return expression.NewLiteral(decimal, types.InternalDecimalType)
	}

	b.handleErr(fmt.Errorf("could not convert %s to any numerical type", value))
	return nil
}

func (b *PlanBuilder) convertVal(ctx *sql.Context, v *sqlparser.SQLVal) sql.Expression {
	switch v.Type {
	case sqlparser.StrVal:
		return expression.NewLiteral(string(v.Val), types.CreateLongText(ctx.GetCollation()))
	case sqlparser.IntVal:
		return b.convertInt(string(v.Val), 10)
	case sqlparser.FloatVal:
		val, err := strconv.ParseFloat(string(v.Val), 64)
		if err != nil {
			b.handleErr(err)
		}

		// use the value as string format to keep precision and scale as defined for DECIMAL data type to avoid rounded up float64 value
		if ps := strings.Split(string(v.Val), "."); len(ps) == 2 {
			ogVal := string(v.Val)
			floatVal := fmt.Sprintf("%v", val)
			if len(ogVal) >= len(floatVal) && ogVal != floatVal {
				p, s := expression.GetDecimalPrecisionAndScale(ogVal)
				dt, err := types.CreateDecimalType(p, s)
				if err != nil {
					return expression.NewLiteral(string(v.Val), types.CreateLongText(ctx.GetCollation()))
				}
				dVal, _, err := dt.Convert(ogVal)
				if err != nil {
					return expression.NewLiteral(string(v.Val), types.CreateLongText(ctx.GetCollation()))
				}
				return expression.NewLiteral(dVal, dt)
			}
		}

		return expression.NewLiteral(val, types.Float64)
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
			b.handleErr(err)
		}
		return expression.NewLiteral(dst, types.LongBlob)
	case sqlparser.HexVal:
		//TODO: binary collation?
		val, err := v.HexDecode()
		if err != nil {
			b.handleErr(err)
		}
		return expression.NewLiteral(val, types.LongBlob)
	case sqlparser.ValArg:
		return expression.NewBindVar(strings.TrimPrefix(string(v.Val), ":"))
	case sqlparser.BitVal:
		if len(v.Val) == 0 {
			return expression.NewLiteral(0, types.Uint64)
		}

		res, err := strconv.ParseUint(string(v.Val), 2, 64)
		if err != nil {
			b.handleErr(err)
		}

		return expression.NewLiteral(res, types.Uint64)
	}

	b.handleErr(sql.ErrInvalidSQLValType.New(v.Type))
	return nil
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
func (b *PlanBuilder) tableSpecToSchema(inScope *scope, tableSpec *sqlparser.TableSpec, forceInvalidCollation bool) (sql.PrimaryKeySchema, sql.CollationID) {
	tableCollation := sql.Collation_Unspecified
	if !forceInvalidCollation {
		if len(tableSpec.Options) > 0 {
			charsetSubmatches := tableCharsetOptionRegex.FindStringSubmatch(tableSpec.Options)
			collationSubmatches := tableCollationOptionRegex.FindStringSubmatch(tableSpec.Options)
			if len(charsetSubmatches) == 5 && len(collationSubmatches) == 5 {
				var err error
				tableCollation, err = sql.ParseCollation(&charsetSubmatches[4], &collationSubmatches[4], false)
				if err != nil {
					return sql.PrimaryKeySchema{}, sql.Collation_Unspecified
				}
			} else if len(charsetSubmatches) == 5 {
				charset, err := sql.ParseCharacterSet(charsetSubmatches[4])
				if err != nil {
					return sql.PrimaryKeySchema{}, sql.Collation_Unspecified
				}
				tableCollation = charset.DefaultCollation()
			} else if len(collationSubmatches) == 5 {
				var err error
				tableCollation, err = sql.ParseCollation(nil, &collationSubmatches[4], false)
				if err != nil {
					return sql.PrimaryKeySchema{}, sql.Collation_Unspecified
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
		column := b.columnDefinitionToColumn(inScope, cd, tableSpec.Indexes)

		if column.PrimaryKey && bool(cd.Type.Null) {
			b.handleErr(ErrPrimaryKeyOnNullField.New())
		}

		schema = append(schema, column)
	}

	return sql.NewPrimaryKeySchema(schema, getPkOrdinals(tableSpec)...), tableCollation
}

// columnDefinitionToColumn returns the sql.Column for the column definition given, as part of a create table statement.
func (b *PlanBuilder) columnDefinitionToColumn(inScope *scope, cd *sqlparser.ColumnDefinition, indexes []*sqlparser.IndexDefinition) *sql.Column {
	internalTyp, err := types.ColumnTypeToType(&cd.Type)
	if err != nil {
		b.handleErr(err)
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

	defaultVal := b.convertDefaultExpression(inScope, cd.Type.Default)

	extra := ""

	if cd.Type.Autoincrement {
		extra = "auto_increment"
	}

	if cd.Type.SRID != nil {
		sridVal, err := strconv.ParseInt(string(cd.Type.SRID.Val), 10, 32)
		if err != nil {
			b.handleErr(err)
		}

		if uint32(sridVal) != types.CartesianSRID && uint32(sridVal) != types.GeoSpatialSRID {
			b.handleErr(sql.ErrUnsupportedFeature.New("unsupported SRID value"))
		}
		if s, ok := internalTyp.(sql.SpatialColumnType); ok {
			internalTyp = s.SetSRID(uint32(sridVal))
		} else {
			b.handleErr(sql.ErrInvalidType.New(fmt.Sprintf("cannot define SRID for %s", internalTyp)))
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
	}
}

func (b *PlanBuilder) convertDefaultExpression(inScope *scope, defaultExpr sqlparser.Expr) *sql.ColumnDefaultValue {
	if defaultExpr == nil {
		return nil
	}
	parsedExpr := b.buildScalar(inScope, defaultExpr)

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
				err := sql.ErrSyntaxError.New("column default function expressions must be enclosed in parentheses")
				b.handleErr(err)
			}
		}
	}

	return &sql.ColumnDefaultValue{
		Expression:    parsedExpr,
		OutType:       nil,
		Literal:       isLiteral,
		ReturnNil:     true,
		Parenthesized: isParenthesized,
	}
}
