package memory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestTableDataSecondaryIndexDeterminism(t *testing.T) {
	require := require.New(t)
	db := NewDatabase("db")
	pro := NewDBProvider(db)
	session := NewSession(sql.NewBaseSession(), pro)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "c0", Type: types.Int64, Source: "test", Nullable: false},
		{Name: "c1", Type: types.Int64, Source: "test", Nullable: false},
		{Name: "pk", Type: types.Int64, Source: "test", PrimaryKey: true, Nullable: false},
	})
	table := NewTable(ctx, db.BaseDatabase, "test", schema, nil)

	err := table.CreateIndex(ctx, sql.IndexDef{
		Name: "idx_c0_c1",
		Columns: []sql.IndexColumn{
			{Name: "c0"},
			{Name: "c1"},
		},
		Constraint: sql.IndexConstraint_None,
	})
	require.NoError(err)

	var expectedPkOrder0 []int64
	var expectedPkOrder1 []int64

	// Insert 100 rows with ties on (c0, c1) but different pk.
	// We interleave (0,0) and (1,1) so that the sorting algorithm has to move elements.
	// Unstable sort (like pdqsort in sort.Slice) will scramble the relative order
	// of tied elements during partitioning.
	// Stable sort will preserve the insertion order perfectly.
	for i := 0; i < 50; i++ {
		pk0 := int64(1000 - i)
		expectedPkOrder0 = append(expectedPkOrder0, pk0)
		require.NoError(table.Insert(ctx, sql.Row{int64(0), int64(0), pk0}))

		pk1 := int64(2000 - i)
		expectedPkOrder1 = append(expectedPkOrder1, pk1)
		require.NoError(table.Insert(ctx, sql.Row{int64(1), int64(1), pk1}))
	}

	td := table.data
	td.sortSecondaryIndexes(ctx) // Sort the secondary indexes manually since we bypassed inserter.Close()

	idxRows, ok := td.secondaryIndexStorage["idx_c0_c1"]
	require.True(ok, "secondary index idx_c0_c1 not found in storage")
	require.Len(idxRows, 100)

	var actualPkOrder0 []int64
	var actualPkOrder1 []int64

	for _, ir := range idxRows {
		c0 := ir[0].(int64)
		pk := ir[2].(int64)
		if c0 == 0 {
			actualPkOrder0 = append(actualPkOrder0, pk)
		} else {
			actualPkOrder1 = append(actualPkOrder1, pk)
		}
	}

	require.Equal(expectedPkOrder0, actualPkOrder0, "secondary index order for (0,0) is not deterministic")
	require.Equal(expectedPkOrder1, actualPkOrder1, "secondary index order for (1,1) is not deterministic")
}
