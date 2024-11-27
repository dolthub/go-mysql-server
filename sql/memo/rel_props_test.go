package memo

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestPopulateFDs(t *testing.T) {
	// source relations
	tests := []struct {
		name    string
		in      RelExpr
		all     sql.ColSet
		notNull sql.ColSet
		indexes []*Index
		key     sql.ColSet
	}{
		{
			name: "tablescan",
			in: &TableScan{
				sourceBase: &sourceBase{relBase: &relBase{}},
				Table: plan.NewResolvedTable(
					&dummyTable{
						schema: sql.NewPrimaryKeySchema(sql.Schema{
							{Name: "x", Source: "t", Type: types.Int64, Nullable: false},
							{Name: "y", Source: "t", Type: types.Int64, Nullable: false},
							{Name: "z", Source: "t", Type: types.Int64, Nullable: false},
						}, 1, 0),
					}, nil, nil).WithId(1).WithColumns(sql.NewColSet(1, 2, 3)),
			},
			all:     sql.NewColSet(1, 2, 3),
			notNull: sql.NewColSet(1, 2, 3),
			indexes: []*Index{
				{
					order: []sql.ColumnId{2, 1},
					set:   sql.NewColSet(1, 2),
				},
			},
			key: sql.NewColSet(1, 2),
		},
		{
			name: "table alias",
			in: &TableAlias{
				sourceBase: &sourceBase{relBase: &relBase{}},
				Table: plan.NewTableAlias("tab", plan.NewResolvedTable(
					&dummyTable{
						schema: sql.NewPrimaryKeySchema(sql.Schema{
							{Name: "x", Source: "t", Type: types.Int64, Nullable: false},
							{Name: "y", Source: "t", Type: types.Int64, Nullable: false},
							{Name: "z", Source: "t", Type: types.Int64, Nullable: false},
						}, 0, 1, 2),
					}, nil, nil).WithId(1).WithColumns(sql.NewColSet(1, 2, 3))),
			},
			all:     sql.NewColSet(1, 2, 3),
			notNull: sql.NewColSet(1, 2, 3),
			indexes: []*Index{
				{
					order: []sql.ColumnId{1, 2, 3},
					set:   sql.NewColSet(1, 2, 3),
				},
			},
			key: sql.NewColSet(1, 2, 3),
		},
		{
			name: "empty table",
			in: &EmptyTable{
				sourceBase: &sourceBase{relBase: &relBase{}},
				Table: plan.NewEmptyTableWithSchema(
					sql.Schema{
						{Name: "x", Source: "t", Type: types.Int64, Nullable: false},
						{Name: "y", Source: "t", Type: types.Int64, Nullable: false},
						{Name: "z", Source: "t", Type: types.Int64, Nullable: false},
					}).(*plan.EmptyTable).WithId(1).WithColumns(sql.NewColSet(1, 2, 3)).(*plan.EmptyTable),
			},
			// planning ignores empty tables for now
			all:     sql.NewColSet(),
			notNull: sql.NewColSet(),
		},
		{
			name: "max1Row",
			in: &Max1Row{
				relBase: &relBase{},
				Child: newExprGroup(NewMemo(nil, nil, nil, 0, nil, nil), 0, &TableScan{
					sourceBase: &sourceBase{relBase: &relBase{}},
					Table: plan.NewResolvedTable(
						&dummyTable{
							schema: sql.NewPrimaryKeySchema(sql.Schema{
								{Name: "x", Source: "t", Type: types.Int64, Nullable: false},
								{Name: "y", Source: "t", Type: types.Int64, Nullable: false},
								{Name: "z", Source: "t", Type: types.Int64, Nullable: false},
							}),
						}, nil, nil).WithId(1).WithColumns(sql.NewColSet(1, 2, 3)),
				},
				),
			},
			all:     sql.NewColSet(1, 2, 3),
			notNull: sql.NewColSet(1, 2, 3),
			key:     sql.ColSet{},
		},
		{
			name: "values",
			in: &Values{
				sourceBase: &sourceBase{relBase: &relBase{}},
				Table:      plan.NewValueDerivedTable(plan.NewValues([][]sql.Expression{{expression.NewLiteral(1, types.Int64)}}), "values").WithId(1).WithColumns(sql.NewColSet(1)).(*plan.ValueDerivedTable),
			},
			all:     sql.NewColSet(1),
			notNull: sql.NewColSet(1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.in.SetGroup(&ExprGroup{First: tt.in, m: NewMemo(nil, nil, nil, 0, nil, nil)})
			props := newRelProps(tt.in)
			require.Equal(t, tt.all, props.fds.All())
			require.Equal(t, tt.notNull, props.fds.NotNull())

			cmp, ok := props.fds.StrictKey()
			if !tt.key.Empty() {
				require.True(t, ok)
				require.Equal(t, tt.key, cmp)
			} else if ok {
				require.True(t, props.fds.HasMax1Row())
			}

			if src, ok := tt.in.(SourceRel); ok {
				cmpIdx := src.Indexes()
				require.Equal(t, len(tt.indexes), len(cmpIdx))
				for i, ii := range tt.indexes {
					cmp := cmpIdx[i]
					require.Equal(t, ii.Cols(), cmp.Cols())
					require.Equal(t, ii.ColSet(), cmp.ColSet())
				}
			}
		})
	}
}

type dummyTable struct {
	schema sql.PrimaryKeySchema
}

var _ sql.Table = (*dummyTable)(nil)
var _ sql.PrimaryKeyTable = (*dummyTable)(nil)
var _ sql.IndexAddressable = (*dummyTable)(nil)

func (t *dummyTable) IndexedAccess(sql.IndexLookup) sql.IndexedTable {
	panic("implement me")
}

func (t *dummyTable) PreciseMatch() bool {
	return true
}

func (t *dummyTable) GetIndexes(*sql.Context) ([]sql.Index, error) {
	var exprs []string
	for _, i := range t.schema.PkOrdinals {
		exprs = append(exprs, fmt.Sprintf("%s.%s", t.Name(), t.schema.Schema[i].Name))
	}
	return []sql.Index{dummyIndex{cols: exprs}}, nil
}

func (t *dummyTable) PrimaryKeySchema() sql.PrimaryKeySchema {
	return t.schema
}

func (t *dummyTable) Name() string { return "dummy" }

func (t *dummyTable) String() string {
	return "name"
}

func (*dummyTable) Insert(*sql.Context, sql.Row) error {
	panic("not implemented")
}

func (t *dummyTable) Schema() sql.Schema { return t.schema.Schema }

func (t *dummyTable) Collation() sql.CollationID { return sql.Collation_Default }

func (t *dummyTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	panic("not implemented")
}

func (t *dummyTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	panic("not implemented")
}

type dummyIndex struct {
	cols []string
}

func (dummyIndex) CanSupport(...sql.Range) bool {
	return true
}

func (dummyIndex) ID() string {
	return "test_index"
}

func (dummyIndex) Database() string {
	return "database"
}

func (dummyIndex) Table() string {
	return "table"
}

func (i dummyIndex) Expressions() []string {
	return i.cols
}

func (dummyIndex) IsUnique() bool {
	return true
}

func (dummyIndex) IsSpatial() bool {
	return false
}

func (dummyIndex) IsFullText() bool {
	return false
}

func (dummyIndex) CanSupportOrderBy(sql.Expression) bool {
	return false
}

func (dummyIndex) Comment() string {
	return ""
}

func (dummyIndex) IndexType() string {
	return "FAKE"
}

func (dummyIndex) IsGenerated() bool {
	return false
}

func (i dummyIndex) ColumnExpressionTypes() []sql.ColumnExpressionType {
	es := i.Expressions()
	res := make([]sql.ColumnExpressionType, len(es))
	for i := range es {
		res[i] = sql.ColumnExpressionType{Expression: es[i], Type: types.Int8}
	}
	return res
}

func (dummyIndex) PrefixLengths() []uint16 {
	return nil
}

var _ sql.Index = dummyIndex{}
