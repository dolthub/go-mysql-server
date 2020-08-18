package plan

import (
	"io"
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

// ErrInsertIntoNotSupported is thrown when a table doesn't support inserts
var ErrInsertIntoNotSupported = errors.NewKind("table doesn't support INSERT INTO")
var ErrReplaceIntoNotSupported = errors.NewKind("table doesn't support REPLACE INTO")
var ErrInsertIntoMismatchValueCount = errors.NewKind("number of values does not match number of columns provided")
var ErrInsertIntoUnsupportedValues = errors.NewKind("%T is unsupported for inserts")
var ErrInsertIntoDuplicateColumn = errors.NewKind("duplicate column name %v")
var ErrInsertIntoNonexistentColumn = errors.NewKind("invalid column name %v")
var ErrInsertIntoNonNullableDefaultNullColumn = errors.NewKind("column name '%v' is non-nullable but attempted to set default value of null")
var ErrInsertIntoNonNullableProvidedNull = errors.NewKind("column name '%v' is non-nullable but attempted to set a value of null")
var ErrInsertIntoIncompatibleTypes = errors.NewKind("cannot convert type %s to %s")

// InsertInto is a node describing the insertion into some table.
type InsertInto struct {
	BinaryNode
	ColumnNames []string
	IsReplace   bool
}

// NewInsertInto creates an InsertInto node.
func NewInsertInto(dst, src sql.Node, isReplace bool, cols []string) *InsertInto {
	return &InsertInto{
		BinaryNode:  BinaryNode{Left: dst, Right: src},
		ColumnNames: cols,
		IsReplace:   isReplace,
	}
}

// Schema implements the Node interface.
func (p *InsertInto) Schema() sql.Schema {
	return sql.OkResultSchema
}

func getInsertable(node sql.Node) (sql.InsertableTable, error) {
	switch node := node.(type) {
	case sql.InsertableTable:
		return node, nil
	case *ResolvedTable:
		return getInsertableTable(node.Table)
	default:
		return nil, ErrInsertIntoNotSupported.New()
	}
}

func getInsertableTable(t sql.Table) (sql.InsertableTable, error) {
	switch t := t.(type) {
	case sql.InsertableTable:
		return t, nil
	case sql.TableWrapper:
		return getInsertableTable(t.Underlying())
	default:
		return nil, ErrInsertIntoNotSupported.New()
	}
}

// Execute inserts the rows in the database.
func (p *InsertInto) Execute(ctx *sql.Context) (int, error) {
	insertable, err := getInsertable(p.Left)
	if err != nil {
		return 0, err
	}

	var replaceable sql.ReplaceableTable
	if p.IsReplace {
		var ok bool
		replaceable, ok = insertable.(sql.ReplaceableTable)
		if !ok {
			return 0, ErrReplaceIntoNotSupported.New()
		}
	}

	dstSchema := p.Left.Schema()

	// If no columns are given, we assume the full schema in order
	if len(p.ColumnNames) == 0 {
		p.ColumnNames = make([]string, len(dstSchema))
		for i, f := range dstSchema {
			p.ColumnNames[i] = f.Name
		}
	} else {
		err = p.validateColumns(dstSchema)
		if err != nil {
			return 0, err
		}
	}

	err = p.validateValueCount(ctx)
	if err != nil {
		return 0, err
	}

	projExprs := make([]sql.Expression, len(dstSchema))
	for i, f := range dstSchema {
		found := false
		for j, col := range p.ColumnNames {
			if f.Name == col {
				projExprs[i] = expression.NewGetField(j, f.Type, f.Name, f.Nullable)
				found = true
				break
			}
		}

		if !found {
			if !f.Nullable && f.Default == nil {
				return 0, ErrInsertIntoNonNullableDefaultNullColumn.New(f.Name)
			}
			projExprs[i] = f.Default
		}
	}

	rowSource, err := p.rowSource(projExprs)
	if err != nil {
		return 0, err
	}

	iter, err := rowSource.RowIter(ctx, nil)
	if err != nil {
		return 0, err
	}

	var inserter sql.RowInserter
	var replacer sql.RowReplacer
	if replaceable != nil {
		replacer = replaceable.Replacer(ctx)
	} else {
		inserter = insertable.Inserter(ctx)
	}

	i := 0
	for {
		row, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = iter.Close()
			return i, err
		}

		err = p.validateNullability(dstSchema, row)
		if err != nil {
			_ = iter.Close()
			return i, err
		}

		// Convert values to the destination schema type
		for colIdx, oldValue := range row {
			dstColType := projExprs[colIdx].Type()

			if oldValue != nil {
				newValue, err := dstColType.Convert(oldValue)
				if err != nil {
					return i, err
				}

				row[colIdx] = newValue
			}
		}

		if replacer != nil {
			if err = replacer.Delete(ctx, row); err != nil {
				if !sql.ErrDeleteRowNotFound.Is(err) {
					_ = iter.Close()
					return i, err
				}
			} else {
				i++
			}

			if err = replacer.Insert(ctx, row); err != nil {
				_ = iter.Close()
				return i, err
			}
		} else {
			if err := inserter.Insert(ctx, row); err != nil {
				_ = iter.Close()
				return i, err
			}
		}
		i++
	}

	if replacer != nil {
		if err := replacer.Close(ctx); err != nil {
			return 0, err
		}
	} else {
		if err := inserter.Close(ctx); err != nil {
			return 0, err
		}
	}

	return i, nil
}

func (p *InsertInto) rowSource(projExprs []sql.Expression) (sql.Node, error) {
	switch n := p.Right.(type) {
	case *Values:
		return NewProject(projExprs, n), nil
	case *ResolvedTable, *Project, *InnerJoin, *Filter:
		if err := assertCompatibleSchemas(projExprs, n.Schema()); err != nil {
			return nil, err
		}
		return NewProject(projExprs, n), nil
	default:
		return nil, ErrInsertIntoUnsupportedValues.New(n)
	}
}

// RowIter implements the Node interface.
func (p *InsertInto) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	updated, err := p.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.NewRow(sql.OkResult{
		RowsAffected: uint64(updated),
	})), nil
}

// WithChildren implements the Node interface.
func (p *InsertInto) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 2)
	}

	return NewInsertInto(children[0], children[1], p.IsReplace, p.ColumnNames), nil
}

func (p InsertInto) String() string {
	pr := sql.NewTreePrinter()
	if p.IsReplace {
		_ = pr.WriteNode("Replace(%s)", strings.Join(p.ColumnNames, ", "))
	} else {
		_ = pr.WriteNode("Insert(%s)", strings.Join(p.ColumnNames, ", "))
	}
	_ = pr.WriteChildren(p.Left.String(), p.Right.String())
	return pr.String()
}

func (p *InsertInto) validateValueCount(ctx *sql.Context) error {
	switch node := p.Right.(type) {
	case *Values:
		for _, exprTuple := range node.ExpressionTuples {
			if len(exprTuple) != len(p.ColumnNames) {
				return ErrInsertIntoMismatchValueCount.New()
			}
		}
	case *ResolvedTable, *Project, *InnerJoin, *Filter:
		return p.assertColumnCountsMatch(node.Schema())
	default:
		return ErrInsertIntoUnsupportedValues.New(node)
	}
	return nil
}

func (p *InsertInto) validateColumns(dstSchema sql.Schema) error {
	dstColNames := make(map[string]struct{})
	for _, dstCol := range dstSchema {
		dstColNames[dstCol.Name] = struct{}{}
	}
	columnNames := make(map[string]struct{})
	for _, columnName := range p.ColumnNames {
		if _, exists := dstColNames[columnName]; !exists {
			return ErrInsertIntoNonexistentColumn.New(columnName)
		}
		if _, exists := columnNames[columnName]; !exists {
			columnNames[columnName] = struct{}{}
		} else {
			return ErrInsertIntoDuplicateColumn.New(columnName)
		}
	}
	return nil
}

func (p *InsertInto) validateNullability(dstSchema sql.Schema, row sql.Row) error {
	for i, col := range dstSchema {
		if !col.Nullable && row[i] == nil {
			return ErrInsertIntoNonNullableProvidedNull.New(col.Name)
		}
	}
	return nil
}

func (p *InsertInto) assertColumnCountsMatch(schema sql.Schema) error {
	if len(p.ColumnNames) != len(schema) {
		return ErrInsertIntoMismatchValueCount.New()
	}
	return nil
}

func assertCompatibleSchemas(projExprs []sql.Expression, schema sql.Schema) error {
	for _, expr := range projExprs {
		switch e := expr.(type) {
		case *expression.Literal:
			continue
		case *expression.GetField:
			otherCol := schema[e.Index()]
			_, err := otherCol.Type.Convert(expr.Type().Zero())
			if err != nil {
				return ErrInsertIntoIncompatibleTypes.New(otherCol.Type.String(), expr.Type().String())
			}
		default:
			return ErrInsertIntoUnsupportedValues.New(expr)
		}
	}
	return nil
}
