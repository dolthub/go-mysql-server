package plan

import (
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var testingTable *mem.Table
var testingTableSize int

func TestLimitPlan(t *testing.T) {
	require := require.New(t)
	table, _ := getTestingTable(t)
	limitPlan := NewLimit(0, NewResolvedTable(table))
	require.Equal(1, len(limitPlan.Children()))

	iterator, err := getLimitedIterator(t, 1)
	require.NoError(err)
	require.NotNil(iterator)
}

func TestLimitImplementsNode(t *testing.T) {
	require := require.New(t)
	table, _ := getTestingTable(t)
	limitPlan := NewLimit(0, NewResolvedTable(table))
	childSchema := table.Schema()
	nodeSchema := limitPlan.Schema()
	require.True(reflect.DeepEqual(childSchema, nodeSchema))
	require.True(receivesNode(limitPlan))
	require.True(limitPlan.Resolved())
}

func TestLimit0(t *testing.T) {
	_, testingTableSize := getTestingTable(t)
	testingLimit := 0
	iterator, _ := getLimitedIterator(t, int64(testingLimit))
	testLimitOverflow(t, iterator, testingLimit, testingTableSize)
}

func TestLimitLessThanTotal(t *testing.T) {
	_, testingTableSize := getTestingTable(t)
	testingLimit := testingTableSize - 1
	iterator, _ := getLimitedIterator(t, int64(testingLimit))
	testLimitOverflow(t, iterator, testingLimit, testingTableSize)
}

func TestLimitEqualThanTotal(t *testing.T) {
	_, testingTableSize := getTestingTable(t)
	testingLimit := testingTableSize
	iterator, _ := getLimitedIterator(t, int64(testingLimit))
	testLimitOverflow(t, iterator, testingLimit, testingTableSize)
}

func TestLimitGreaterThanTotal(t *testing.T) {
	_, testingTableSize := getTestingTable(t)
	testingLimit := testingTableSize + 1
	iterator, _ := getLimitedIterator(t, int64(testingLimit))
	testLimitOverflow(t, iterator, testingLimit, testingTableSize)
}

func testLimitOverflow(t *testing.T, iter sql.RowIter, limit int, dataSize int) {
	require := require.New(t)
	for i := 0; i < limit+1; i++ {
		row, err := iter.Next()
		hint := fmt.Sprintf("Iter#%d : size.%d : limit.%d", i, dataSize, limit)
		if i >= limit || i >= dataSize {
			require.Nil(row, hint)
			require.Equal(io.EOF, err, hint)
		} else {
			require.NotNil(row, hint)
			require.Nil(err, hint)
		}
	}
}

func getTestingTable(t *testing.T) (*mem.Table, int) {
	t.Helper()
	if &testingTable == nil {
		return testingTable, testingTableSize
	}

	childSchema := sql.Schema{
		{Name: "col1", Type: sql.Text},
	}
	testingTable = mem.NewTable("test", childSchema)

	rows := []sql.Row{
		sql.NewRow("11a"),
		sql.NewRow("22a"),
		sql.NewRow("33a"),
	}

	for _, r := range rows {
		require.NoError(t, testingTable.Insert(sql.NewEmptyContext(), r))
	}

	return testingTable, len(rows)
}

func getLimitedIterator(t *testing.T, limitSize int64) (sql.RowIter, error) {
	t.Helper()
	ctx := sql.NewEmptyContext()
	table, _ := getTestingTable(t)
	limitPlan := NewLimit(limitSize, NewResolvedTable(table))
	return limitPlan.RowIter(ctx)
}

func receivesNode(n sql.Node) bool {
	return true
}
