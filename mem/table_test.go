package mem

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestTablePartitionsCount(t *testing.T) {
	require := require.New(t)
	table := NewPartitionedTable("foo", nil, 5)
	count, err := table.PartitionCount(sql.NewEmptyContext())
	require.NoError(err)
	require.Equal(int64(5), count)
}

func TestTableName(t *testing.T) {
	require := require.New(t)
	s := sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
	}

	table := NewTable("test", s)
	require.Equal("test", table.name)
}

const expectedString = `Table(foo)
 ├─ Column(col1, TEXT, nullable=true)
 └─ Column(col2, INT64, nullable=false)
`

func TestTableString(t *testing.T) {
	require := require.New(t)
	table := NewTable("foo", sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
		{Name: "col2", Type: sql.Int64, Nullable: false},
	})
	require.Equal(expectedString, table.String())
}

type indexKeyValue struct {
	key   sql.Row
	value *indexValue
}

type dummyLookup struct {
	values map[string][]*indexValue
}

func (dummyLookup) Indexes() []string { return nil }

func (i *dummyLookup) Values(partition sql.Partition) (sql.IndexValueIter, error) {
	key := string(partition.Key())
	values, ok := i.values[key]
	if !ok {
		return nil, fmt.Errorf("wrong partition key %q", key)
	}

	return &dummyLookupIter{values: values}, nil
}

type dummyLookupIter struct {
	values []*indexValue
	pos    int
}

var _ sql.IndexValueIter = (*dummyLookupIter)(nil)

func (i *dummyLookupIter) Next() ([]byte, error) {
	if i.pos >= len(i.values) {
		return nil, io.EOF
	}

	value := i.values[i.pos]
	i.pos++
	return encodeIndexValue(value)
}

func (i *dummyLookupIter) Close() error { return nil }

var tests = []struct {
	name          string
	schema        sql.Schema
	numPartitions int
	rows          []sql.Row

	filters          []sql.Expression
	expectedFiltered []sql.Row

	columns           []string
	expectedSchema    sql.Schema
	expectedProjected []sql.Row

	expectedFiltersAndProjections []sql.Row

	indexColumns      []string
	expectedKeyValues []*indexKeyValue

	lookup          *dummyLookup
	partition       *partition
	expectedIndexed []sql.Row
}{
	{
		name: "test",
		schema: sql.Schema{
			&sql.Column{Name: "col1", Source: "test", Type: sql.Text, Nullable: false, Default: ""},
			&sql.Column{Name: "col2", Source: "test", Type: sql.Int32, Nullable: false, Default: int32(0)},
			&sql.Column{Name: "col3", Source: "test", Type: sql.Int64, Nullable: false, Default: int64(0)},
		},
		numPartitions: 2,
		rows: []sql.Row{
			sql.NewRow("a", int32(10), int64(100)),
			sql.NewRow("b", int32(10), int64(100)),
			sql.NewRow("c", int32(20), int64(100)),
			sql.NewRow("d", int32(20), int64(200)),
			sql.NewRow("e", int32(10), int64(200)),
			sql.NewRow("f", int32(20), int64(200)),
		},
		filters: []sql.Expression{
			expression.NewEquals(
				expression.NewGetFieldWithTable(1, sql.Int32, "test", "col2", false),
				expression.NewLiteral(int32(10), sql.Int32),
			),
		},
		expectedFiltered: []sql.Row{
			sql.NewRow("a", int32(10), int64(100)),
			sql.NewRow("b", int32(10), int64(100)),
			sql.NewRow("e", int32(10), int64(200)),
		},
		columns: []string{"col3", "col1"},
		expectedSchema: sql.Schema{
			&sql.Column{Name: "col3", Source: "test", Type: sql.Int64, Nullable: false, Default: int64(0)},
			&sql.Column{Name: "col1", Source: "test", Type: sql.Text, Nullable: false, Default: ""},
		},
		expectedProjected: []sql.Row{
			sql.NewRow(int64(100), "a"),
			sql.NewRow(int64(100), "b"),
			sql.NewRow(int64(100), "c"),
			sql.NewRow(int64(200), "d"),
			sql.NewRow(int64(200), "e"),
			sql.NewRow(int64(200), "f"),
		},
		expectedFiltersAndProjections: []sql.Row{
			sql.NewRow(int64(100), "a"),
			sql.NewRow(int64(100), "b"),
			sql.NewRow(int64(200), "e"),
		},
		indexColumns: []string{"col1", "col3"},
		expectedKeyValues: []*indexKeyValue{
			&indexKeyValue{sql.NewRow("a", int64(100)), &indexValue{Key: "0", Pos: 0}},
			&indexKeyValue{sql.NewRow("c", int64(100)), &indexValue{Key: "0", Pos: 1}},
			&indexKeyValue{sql.NewRow("e", int64(200)), &indexValue{Key: "0", Pos: 2}},
			&indexKeyValue{sql.NewRow("b", int64(100)), &indexValue{Key: "1", Pos: 0}},
			&indexKeyValue{sql.NewRow("d", int64(200)), &indexValue{Key: "1", Pos: 1}},
			&indexKeyValue{sql.NewRow("f", int64(200)), &indexValue{Key: "1", Pos: 2}},
		},
		lookup: &dummyLookup{
			values: map[string][]*indexValue{
				"0": []*indexValue{
					&indexValue{Key: "0", Pos: 0},
					&indexValue{Key: "0", Pos: 1},
					&indexValue{Key: "0", Pos: 2},
				},
				"1": []*indexValue{
					&indexValue{Key: "1", Pos: 0},
					&indexValue{Key: "1", Pos: 1},
					&indexValue{Key: "1", Pos: 2},
				},
			},
		},
		partition: &partition{key: []byte("0")},
		expectedIndexed: []sql.Row{
			sql.NewRow(int64(100), "a"),
			sql.NewRow(int64(200), "e"),
		},
	},
}

func TestTable(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var require = require.New(t)

			table := NewPartitionedTable(test.name, test.schema, test.numPartitions)
			for _, row := range test.rows {
				require.NoError(table.Insert(sql.NewEmptyContext(), row))
			}

			pIter, err := table.Partitions(sql.NewEmptyContext())
			require.NoError(err)

			for i := 0; i < test.numPartitions; i++ {
				p, err := pIter.Next()
				require.NoError(err)

				iter, err := table.PartitionRows(sql.NewEmptyContext(), p)
				require.NoError(err)

				rows, err := sql.RowIterToRows(iter)
				require.NoError(err)

				key := string(p.Key())
				expected := table.partitions[key]
				require.Len(rows, len(expected))

				for i, row := range rows {
					require.ElementsMatch(expected[i], row)
				}
			}

			_, err = pIter.Next()
			require.EqualError(err, io.EOF.Error())

		})
	}
}

func TestFiltered(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var require = require.New(t)

			table := NewPartitionedTable(test.name, test.schema, test.numPartitions)
			for _, row := range test.rows {
				require.NoError(table.Insert(sql.NewEmptyContext(), row))
			}

			filtered := table.WithFilters(test.filters)

			filteredRows := testFlatRows(t, filtered)
			require.Len(filteredRows, len(test.expectedFiltered))
			for _, row := range filteredRows {
				require.Contains(test.expectedFiltered, row)
			}

		})
	}
}

func TestProjected(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var require = require.New(t)

			table := NewPartitionedTable(test.name, test.schema, test.numPartitions)
			for _, row := range test.rows {
				require.NoError(table.Insert(sql.NewEmptyContext(), row))
			}

			projected := table.WithProjection(test.columns)
			require.ElementsMatch(projected.Schema(), test.expectedSchema)

			projectedRows := testFlatRows(t, projected)
			require.Len(projectedRows, len(test.expectedProjected))
			for _, row := range projectedRows {
				require.Contains(test.expectedProjected, row)
			}
		})
	}
}

func TestFilterAndProject(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var require = require.New(t)

			table := NewPartitionedTable(test.name, test.schema, test.numPartitions)
			for _, row := range test.rows {
				require.NoError(table.Insert(sql.NewEmptyContext(), row))
			}

			filtered := table.WithFilters(test.filters)
			projected := filtered.(*Table).WithProjection(test.columns)
			require.ElementsMatch(projected.Schema(), test.expectedSchema)

			rows := testFlatRows(t, projected)
			require.Len(rows, len(test.expectedFiltersAndProjections))
			for _, row := range rows {
				require.Contains(test.expectedFiltersAndProjections, row)
			}
		})
	}
}

func TestIndexed(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var require = require.New(t)

			table := NewPartitionedTable(test.name, test.schema, test.numPartitions)
			for _, row := range test.rows {
				require.NoError(table.Insert(sql.NewEmptyContext(), row))
			}

			filtered := table.WithFilters(test.filters)
			projected := filtered.(*Table).WithProjection(test.columns)
			indexed := projected.(*Table).WithIndexLookup(test.lookup)

			require.ElementsMatch(indexed.Schema(), test.expectedSchema)

			iter, err := indexed.PartitionRows(sql.NewEmptyContext(), test.partition)
			require.NoError(err)

			rows, err := sql.RowIterToRows(iter)
			require.NoError(err)

			require.Len(rows, len(test.expectedIndexed))
			for _, row := range rows {
				require.Contains(test.expectedIndexed, row)
			}
		})
	}
}

func testFlatRows(t *testing.T, table sql.Table) []sql.Row {
	var require = require.New(t)

	pIter, err := table.Partitions(sql.NewEmptyContext())
	require.NoError(err)
	flatRows := []sql.Row{}
	for {
		p, err := pIter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}

			require.NoError(err)
		}

		iter, err := table.PartitionRows(sql.NewEmptyContext(), p)
		require.NoError(err)

		rows, err := sql.RowIterToRows(iter)
		require.NoError(err)

		flatRows = append(flatRows, rows...)

	}

	return flatRows
}

func TestTableIndexKeyValueIter(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var require = require.New(t)

			table := NewPartitionedTable(test.name, test.schema, test.numPartitions)
			for _, row := range test.rows {
				require.NoError(table.Insert(sql.NewEmptyContext(), row))
			}

			pIter, err := table.IndexKeyValues(
				sql.NewEmptyContext(),
				[]string{test.schema[0].Name, test.schema[2].Name},
			)
			require.NoError(err)

			var iter sql.IndexKeyValueIter
			idxKVs := []*indexKeyValue{}
			for {
				if iter == nil {
					_, iter, err = pIter.Next()
					if err != nil {
						if err == io.EOF {
							iter = nil
							break
						}

						require.NoError(err)
					}
				}

				row, data, err := iter.Next()
				if err != nil {
					if err == io.EOF {
						iter = nil
						continue
					}

					require.NoError(err)
				}

				value, err := decodeIndexValue(data)
				require.NoError(err)

				idxKVs = append(idxKVs, &indexKeyValue{key: row, value: value})
			}

			require.Len(idxKVs, len(test.expectedKeyValues))
			for i, e := range test.expectedKeyValues {
				require.Equal(e, idxKVs[i])
			}
		})
	}
}
