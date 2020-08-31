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
	return p.Left.Schema()
}

func getInsertable(node sql.Node) (sql.InsertableTable, error) {
	switch node := node.(type) {
	case *Exchange:
		return getInsertable(node.Child)
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

func (p *InsertInto) rowSource(projExprs []sql.Expression) (sql.Node, error) {
	right := p.Right
	if exchange, ok := right.(*Exchange); ok {
		right = exchange.Child
	}

	switch n := right.(type) {
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

func validateColumns(columnNames []string, dstSchema sql.Schema) error {
	dstColNames := make(map[string]struct{})
	for _, dstCol := range dstSchema {
		dstColNames[dstCol.Name] = struct{}{}
	}
	usedNames := make(map[string]struct{})
	for _, columnName := range columnNames {
		if _, exists := dstColNames[columnName]; !exists {
			return ErrInsertIntoNonexistentColumn.New(columnName)
		}
		if _, exists := usedNames[columnName]; !exists {
			usedNames[columnName] = struct{}{}
		} else {
			return ErrInsertIntoDuplicateColumn.New(columnName)
		}
	}
	return nil
}

func validateValueCount(columnNames []string, values sql.Node) error {
	right := values
	if exchange, ok := right.(*Exchange); ok {
		right = exchange.Child
	}

	switch node := right.(type) {
	case *Values:
		for _, exprTuple := range node.ExpressionTuples {
			if len(exprTuple) != len(columnNames) {
				return ErrInsertIntoMismatchValueCount.New()
			}
		}
	case *ResolvedTable, *Project, *InnerJoin, *Filter:
		if len(columnNames) != len(right.Schema()) {
			return ErrInsertIntoMismatchValueCount.New()
		}
	default:
		return ErrInsertIntoUnsupportedValues.New(node)
	}
	return nil
}

type insertIter struct {
	schema      sql.Schema
	inserter    sql.RowInserter
	replacer    sql.RowReplacer
	rowSource   sql.RowIter
	projections []sql.Expression
	ctx         *sql.Context
}

func newInsertIter(ctx *sql.Context, table sql.Node, values sql.Node, columnNames []string, isReplace bool) (*insertIter, error) {
	insertable, err := getInsertable(table)
	if err != nil {
		return nil, err
	}

	var replaceable sql.ReplaceableTable
	if isReplace {
		var ok bool
		replaceable, ok = insertable.(sql.ReplaceableTable)
		if !ok {
			return nil, ErrReplaceIntoNotSupported.New()
		}
	}

	dstSchema := insertable.Schema()

	// If no columns are given, we assume the full schema in order
	if len(columnNames) == 0 {
		columnNames = make([]string, len(dstSchema))
		for i, f := range dstSchema {
			columnNames[i] = f.Name
		}
	} else {
		err = validateColumns(columnNames, dstSchema)
		if err != nil {
			return nil, err
		}
	}

	err = validateValueCount(columnNames, values)
	if err != nil {
		return nil, err
	}

	projExprs := make([]sql.Expression, len(dstSchema))
	for i, f := range dstSchema {
		found := false
		for j, col := range columnNames {
			if f.Name == col {
				projExprs[i] = expression.NewGetField(j, f.Type, f.Name, f.Nullable)
				found = true
				break
			}
		}

		if !found {
			if !f.Nullable && f.Default == nil {
				return nil, ErrInsertIntoNonNullableDefaultNullColumn.New(f.Name)
			}
			projExprs[i] = expression.NewLiteral(f.Default, f.Type)
		}
	}

	rowSource, err := rowSource(values, projExprs)
	if err != nil {
		return nil, err
	}

	var inserter sql.RowInserter
	var replacer sql.RowReplacer
	if replaceable != nil {
		replacer = replaceable.Replacer(ctx)
	} else {
		inserter = insertable.Inserter(ctx)
	}

	rowIter, err := rowSource.RowIter(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &insertIter{
		schema:      dstSchema,
		inserter:    inserter,
		replacer:    replacer,
		rowSource:   rowIter,
		projections: projExprs,
		ctx:         ctx,
	}, nil
}

func rowSource(values sql.Node, projExprs []sql.Expression) (sql.Node, error) {
	right := values
	if exchange, ok := right.(*Exchange); ok {
		right = exchange.Child
	}

	switch n := right.(type) {
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

func (i insertIter) Next() (sql.Row, error) {
	row, err := i.rowSource.Next()
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		_ = i.rowSource.Close()
		return nil, err
	}

	err = validateNullability(i.schema, row)
	if err != nil {
		_ = i.rowSource.Close()
		return nil, err
	}

	// Convert values to the destination schema type
	for colIdx, oldValue := range row {
		dstColType := i.projections[colIdx].Type()

		if oldValue != nil {
			newValue, err := dstColType.Convert(oldValue)
			if err != nil {
				return nil, err
			}

			row[colIdx] = newValue
		}
	}

	if i.replacer != nil {
		if err = i.replacer.Delete(i.ctx, row); err != nil {
			if !sql.ErrDeleteRowNotFound.Is(err) {
				_ = i.rowSource.Close()
				return nil, err
			}
		}
		// TODO: row update count should go up here

		if err = i.replacer.Insert(i.ctx, row); err != nil {
			_ = i.rowSource.Close()
			return nil, err
		}
	} else {
		if err := i.inserter.Insert(i.ctx, row); err != nil {
			_ = i.rowSource.Close()
			return nil, err
		}
	}

	return row, nil
}

func (i insertIter) Close() error {
	if i.inserter != nil {
		if err := i.inserter.Close(i.ctx); err != nil {
			return err
		}
	}
	if i.replacer != nil {
		if err := i.replacer.Close(i.ctx); err != nil {
			return err
		}
	}
	if i.rowSource != nil {
		if err := i.rowSource.Close(); err != nil {
			return err
		}
	}

	return nil
}

// RowIter implements the Node interface.
func (p *InsertInto) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return newInsertIter(ctx, p.Left, p.Right, p.ColumnNames, p.IsReplace)
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

func (p InsertInto) DebugString() string {
	pr := sql.NewTreePrinter()
	if p.IsReplace {
		_ = pr.WriteNode("Replace(%s)", strings.Join(p.ColumnNames, ", "))
	} else {
		_ = pr.WriteNode("Insert(%s)", strings.Join(p.ColumnNames, ", "))
	}
	_ = pr.WriteChildren(sql.DebugString(p.Left), sql.DebugString(p.Right))
	return pr.String()
}

func validateNullability(dstSchema sql.Schema, row sql.Row) error {
	for i, col := range dstSchema {
		if !col.Nullable && row[i] == nil {
			return ErrInsertIntoNonNullableProvidedNull.New(col.Name)
		}
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
