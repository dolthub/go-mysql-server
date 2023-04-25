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
			floatVal := fmt.Sprintf("%v", val)
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
		if uint32(sridVal) != types.CartesianSRID && uint32(sridVal) != types.GeoSpatialSRID {
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
