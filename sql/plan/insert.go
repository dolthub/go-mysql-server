package plan

import (
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"gopkg.in/src-d/go-errors.v1"
	"io"
	"strings"
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

// InsertInto is a node describing the insertion into some table.
type InsertInto struct {
	BinaryNode
	Columns   []string
	IsReplace bool
}

// NewInsertInto creates an InsertInto node.
func NewInsertInto(dst, src sql.Node, isReplace bool, cols []string) *InsertInto {
	return &InsertInto{
		BinaryNode: BinaryNode{Left: dst, Right: src},
		Columns:    cols,
		IsReplace:  isReplace,
	}
}

// Schema implements the Node interface.
func (p *InsertInto) Schema() sql.Schema {
	return sql.Schema{{
		Name:     "updated",
		Type:     sql.Int64,
		Default:  int64(0),
		Nullable: false,
	}}
}

func getInsertable(node sql.Node) (sql.Inserter, error) {
	switch node := node.(type) {
	case sql.Inserter:
		return node, nil
	case *ResolvedTable:
		return getInsertableTable(node.Table)
	default:
		return nil, ErrInsertIntoNotSupported.New()
	}
}

func getInsertableTable(t sql.Table) (sql.Inserter, error) {
	switch t := t.(type) {
	case sql.Inserter:
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

	var replaceable sql.Replacer
	if p.IsReplace {
		var ok bool
		replaceable, ok = insertable.(sql.Replacer)
		if !ok {
			return 0, ErrReplaceIntoNotSupported.New()
		}
	}

	dstSchema := p.Left.Schema()
	projExprs := make([]sql.Expression, len(dstSchema))

	// If no columns are given, we assume the full schema in order
	if len(p.Columns) == 0 {
		p.Columns = make([]string, len(dstSchema))
		for i, f := range dstSchema {
			p.Columns[i] = f.Name
		}
	} else {
		err = p.validateColumns(ctx, dstSchema)
		if err != nil {
			return 0, err
		}
	}

	err = p.validateValueCount(ctx)
	if err != nil {
		return 0, err
	}

	for i, f := range dstSchema {
		found := false
		for j, col := range p.Columns {
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
			projExprs[i] = expression.NewLiteral(f.Default, f.Type)
		}
	}

	proj := NewProject(projExprs, p.Right)

	iter, err := proj.RowIter(ctx)
	if err != nil {
		return 0, err
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

		err = p.validateNullability(ctx, dstSchema, row)
		if err != nil {
			_ = iter.Close()
			return i, err
		}

		if replaceable != nil {
			if err = replaceable.Delete(ctx, row); err != nil {
				if err != sql.ErrDeleteRowNotFound {
					_ = iter.Close()
					return i, err
				}
			} else {
				i++
			}

			if err = replaceable.Insert(ctx, row); err != nil {
				_ = iter.Close()
				return i, err
			}
		} else {
			if err := insertable.Insert(ctx, row); err != nil {
				_ = iter.Close()
				return i, err
			}
		}
		i++
	}

	return i, nil
}

// RowIter implements the Node interface.
func (p *InsertInto) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	n, err := p.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.NewRow(int64(n))), nil
}

// WithChildren implements the Node interface.
func (p *InsertInto) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 2)
	}

	return NewInsertInto(children[0], children[1], p.IsReplace, p.Columns), nil
}

func (p InsertInto) String() string {
	pr := sql.NewTreePrinter()
	if p.IsReplace {
		_ = pr.WriteNode("Replace(%s)", strings.Join(p.Columns, ", "))
	} else {
		_ = pr.WriteNode("Insert(%s)", strings.Join(p.Columns, ", "))
	}
	_ = pr.WriteChildren(p.Left.String(), p.Right.String())
	return pr.String()
}

func (p *InsertInto) validateValueCount(ctx *sql.Context) error {
	switch node := p.Right.(type) {
	case *Values:
		for _, exprTuple := range node.ExpressionTuples {
			if len(exprTuple) != len(p.Columns) {
				return ErrInsertIntoMismatchValueCount.New()
			}
		}
	default:
		return ErrInsertIntoUnsupportedValues.New(node)
	}
	return nil
}

func (p *InsertInto) validateColumns(ctx *sql.Context, dstSchema sql.Schema) error {
	dstColNames := make(map[string]struct{})
	for _, dstCol := range dstSchema {
		dstColNames[dstCol.Name] = struct{}{}
	}
	columnNames := make(map[string]struct{})
	for _, columnName := range p.Columns {
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

func (p *InsertInto) validateNullability(ctx *sql.Context, dstSchema sql.Schema, row sql.Row) error {
	for i, col := range dstSchema {
		if !col.Nullable && row[i] == nil {
			return ErrInsertIntoNonNullableProvidedNull.New(col.Name)
		}
	}
	return nil
}
