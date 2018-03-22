package plan

import (
	"context"
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
	table, _ := getTestingTable()
	limitPlan := NewLimit(0, table)
	require.Equal(1, len(limitPlan.Children()))

	iterator, err := getLimitedIterator(1)
	require.Nil(err)
	require.NotNil(iterator)
}

func TestLimitImplementsNode(t *testing.T) {
	require := require.New(t)
	table, _ := getTestingTable()
	limitPlan := NewLimit(0, table)
	childSchema := table.Schema()
	nodeSchema := limitPlan.Schema()
	require.True(reflect.DeepEqual(childSchema, nodeSchema))
	require.True(receivesNode(limitPlan))
	require.True(limitPlan.Resolved())
}

func TestLimit0(t *testing.T) {
	_, testingTableSize := getTestingTable()
	testingLimit := 0
	iterator, _ := getLimitedIterator(int64(testingLimit))
	testLimitOverflow(t, iterator, testingLimit, testingTableSize)
}

func TestLimitLessThanTotal(t *testing.T) {
	_, testingTableSize := getTestingTable()
	testingLimit := testingTableSize - 1
	iterator, _ := getLimitedIterator(int64(testingLimit))
	testLimitOverflow(t, iterator, testingLimit, testingTableSize)
}

func TestLimitEqualThanTotal(t *testing.T) {
	_, testingTableSize := getTestingTable()
	testingLimit := testingTableSize
	iterator, _ := getLimitedIterator(int64(testingLimit))
	testLimitOverflow(t, iterator, testingLimit, testingTableSize)
}

func TestLimitGreaterThanTotal(t *testing.T) {
	_, testingTableSize := getTestingTable()
	testingLimit := testingTableSize + 1
	iterator, _ := getLimitedIterator(int64(testingLimit))
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

func getTestingTable() (*mem.Table, int) {

	if &testingTable == nil {
		return testingTable, testingTableSize
	}

	childSchema := sql.Schema{
		{Name: "col1", Type: sql.Text},
	}
	testingTable = mem.NewTable("test", childSchema)
	testingTable.Insert(sql.NewRow("11a"))
	testingTable.Insert(sql.NewRow("22a"))
	testingTable.Insert(sql.NewRow("33a"))
	testingTableSize = 3

	return testingTable, testingTableSize
}

func getLimitedIterator(limitSize int64) (sql.RowIter, error) {
	ctx := sql.NewContext(context.TODO(), sql.NewBaseSession())
	table, _ := getTestingTable()
	limitPlan := NewLimit(limitSize, table)
	return limitPlan.RowIter(ctx)
}

func receivesNode(n sql.Node) bool {
	return true
}
