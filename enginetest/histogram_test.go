package enginetest

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func init() {
	rand.Seed(0)
}

type statsTest struct {
	name       string
	tableGen   func(*sql.Context, *memory.Database, int, sql.TableId, sql.ColumnId, ...interface{}) *plan.ResolvedTable
	args1      []interface{}
	args2      []interface{}
	leftOrd    []int
	rightOrd   []int
	leftTypes  []sql.Type
	rightTypes []sql.Type
	err        float64
}

func TestNormDist(t *testing.T) {
	tests := []struct {
		name  string
		mean1 float64
		std1  float64
		mean2 float64
		std2  float64
	}{
		{
			name:  "same table",
			mean1: 0,
			std1:  10,
			mean2: 0,
			std2:  10,
		},
		{
			name:  "same mean, different std",
			mean1: 0,
			std1:  10,
			mean2: 0,
			std2:  2,
		},
		{
			name:  "peaks don't overlap",
			mean1: 10,
			std1:  5,
			mean2: -10,
			std2:  5,
		},
		//
		{
			name:  "similar mean, different std1",
			mean1: 1,
			std1:  10,
			mean2: 0,
			std2:  2,
		},
		{
			name:  "same mean, different std2",
			mean1: 0,
			std1:  8,
			mean2: 0,
			std2:  2,
		},
		{
			name:  "same mean, different std3",
			mean1: 0,
			std1:  7,
			mean2: 0,
			std2:  2,
		},
		{
			name:  "similar mean, different std4",
			mean1: 1,
			std1:  7,
			mean2: 0,
			std2:  2,
		},
		{
			name:  "similar mean, different std5",
			mean1: 2,
			std1:  7,
			mean2: 0,
			std2:  2,
		},
		{
			name:  "similar mean, different std6",
			mean1: 3,
			std1:  7,
			mean2: 0,
			std2:  2,
		},
		{
			name:  "similar mean, different std7",
			mean1: 4,
			std1:  7,
			mean2: 0,
			std2:  2,
		},
		{
			name:  "similar mean, different std8",
			mean1: 4,
			std1:  7,
			mean2: 0,
			std2:  3,
		},
		{
			name:  "similar mean, different std9",
			mean1: 5,
			std1:  7,
			mean2: 0,
			std2:  3,
		},
	}

	var statTests []statsTest
	for _, t := range tests {
		st := statsTest{
			name: t.name,
			tableGen: func(ctx *sql.Context, db *memory.Database, cnt int, tab sql.TableId, col sql.ColumnId, args ...interface{}) *plan.ResolvedTable {
				mean := args[0].(float64)
				std := args[1].(float64)
				xyz := makeTable(db, fmt.Sprintf("xyz%d", tab), tab, col)
				err := normalDistForTable(ctx, xyz, cnt, mean, std)
				if err != nil {
					panic(err)
				}
				return xyz
			},
			leftOrd:    []int{1},
			rightOrd:   []int{1},
			leftTypes:  []sql.Type{types.Int64, types.Int64, types.Int64},
			rightTypes: []sql.Type{types.Int64, types.Int64, types.Int64},
			err:        1,
			args1:      []interface{}{t.mean1, t.std1},
			args2:      []interface{}{t.mean2, t.std2},
		}
		statTests = append(statTests, st)
	}

	debug := false
	runStatsSuite(t, statTests, 100, 5, debug)
	runStatsSuite(t, statTests, 100, 10, debug)
	runStatsSuite(t, statTests, 100, 20, debug)
	runStatsSuite(t, statTests, 500, 10, debug)
	runStatsSuite(t, statTests, 500, 20, debug)
}

func TestExpDist(t *testing.T) {
	tests := []struct {
		name    string
		lambda1 float64
		lambda2 float64
	}{
		{
			name:    "same table",
			lambda1: 1,
			lambda2: 1,
		},
		{
			name:    ".5/1.5",
			lambda1: .5,
			lambda2: 1.5,
		},
		{
			name:    ".25/3",
			lambda1: .25,
			lambda2: 3,
		},
	}

	var statTests []statsTest
	for _, tt := range tests {
		st := statsTest{
			name: tt.name,
			tableGen: func(ctx *sql.Context, db *memory.Database, cnt int, tab sql.TableId, col sql.ColumnId, args ...interface{}) *plan.ResolvedTable {
				xyz := makeTable(db, "xyz", tab, col)
				err := expDistForTable(ctx, xyz, cnt, args[0].(float64))
				if err != nil {
					panic(err)
				}
				return xyz
			},
			leftOrd:    []int{1},
			rightOrd:   []int{1},
			leftTypes:  []sql.Type{types.Int64, types.Int64, types.Int64},
			rightTypes: []sql.Type{types.Int64, types.Int64, types.Int64},
			args1:      []interface{}{tt.lambda1},
			args2:      []interface{}{tt.lambda2},
			err:        1,
		}
		statTests = append(statTests, st)
	}

	debug := false
	runStatsSuite(t, statTests, 100, 5, debug)
	runStatsSuite(t, statTests, 100, 10, debug)
	runStatsSuite(t, statTests, 100, 20, debug)
	runStatsSuite(t, statTests, 500, 10, debug)
	runStatsSuite(t, statTests, 500, 20, debug)
}

func TestMultiDist(t *testing.T) {
	tests := []statsTest{
		{
			name: "uniform dist int",
			tableGen: func(ctx *sql.Context, db *memory.Database, cnt int, tab sql.TableId, col sql.ColumnId, args ...interface{}) *plan.ResolvedTable {
				xyz := makeTable(db, "xyz", tab, col)
				err := uniformDistForTable(ctx, xyz, cnt)
				if err != nil {
					panic(err)
				}
				return xyz
			},
			leftOrd:    []int{1},
			rightOrd:   []int{1},
			leftTypes:  []sql.Type{types.Int64, types.Int64, types.Int64},
			rightTypes: []sql.Type{types.Int64, types.Int64, types.Int64},
			err:        .1,
		},
	}

	runStatsSuite(t, tests, 1000, 10, false)
}

// runStatsSuite will parse each statsTest and (1) generate 2 tables for a
// join, (2) compute histograms for the tables on the join index, (3) use
// the stats join algo to simulate a join estimate, and (4) compare the
// estimate to the actual result set count.
func runStatsSuite(t *testing.T, tests []statsTest, rowCnt, bucketCnt int, debug bool) {
	for i, tt := range tests {
		t.Run(fmt.Sprintf("%s: , rows: %d, buckets: %d", tt.name, rowCnt, bucketCnt), func(t *testing.T) {
			db := memory.NewDatabase(fmt.Sprintf("test%d", i))
			pro := memory.NewDBProvider(db)

			xyz := tt.tableGen(newContext(pro), db, rowCnt, sql.TableId(i*2), 1, tt.args1...)
			wuv := tt.tableGen(newContext(pro), db, rowCnt, sql.TableId(i*2+1), sql.ColumnId(len(tt.leftTypes)+1), tt.args2...)

			// join the histograms on a particular field
			var joinOps []sql.Expression
			for i, l := range tt.leftOrd {
				r := tt.rightOrd[i]
				joinOps = append(joinOps, eq(l, r+len(tt.leftTypes)))
			}

			exp, err := expectedResultSize(newContext(pro), xyz, wuv, joinOps, debug)
			require.NoError(t, err)

			// get histograms for tables
			xHist, err := testHistogram(newContext(pro), xyz, tt.leftOrd, bucketCnt)
			require.NoError(t, err)

			wHist, err := testHistogram(newContext(pro), wuv, tt.rightOrd, bucketCnt)
			require.NoError(t, err)

			if debug {
				log.Printf("xyz:\n%s\n", sql.Histogram(xHist).DebugString())
				log.Printf("wuv:\n%s\n", sql.Histogram(wHist).DebugString())
			}

			lStat := &stats.Statistic{Typs: []sql.Type{types.Int64}}
			for _, b := range xHist {
				lStat.Hist = append(lStat.Hist, b.(*stats.Bucket))
			}
			rStat := &stats.Statistic{Typs: []sql.Type{types.Int64}}
			for _, b := range wHist {
				rStat.Hist = append(rStat.Hist, b.(*stats.Bucket))
			}

			res, err := stats.Join(stats.UpdateCounts(lStat), stats.UpdateCounts(rStat), 1, debug)
			require.NoError(t, err)
			if debug {
				log.Printf("join %s\n", res.Histogram().DebugString())
			}

			denom := float64(exp)
			if cmp := float64(res.RowCount()); cmp > denom {
				denom = cmp
			}
			delta := float64(exp-int(res.RowCount())) / denom
			if delta < 0 {
				delta = -delta
			}
			if debug {
				log.Println(res.RowCount(), exp, delta)
			}

			// This compares the error percentage for our estimate to an
			// error threshold specified in the statTest. The error bounds
			// are loose and mostly useful for debugging at this point.
			require.Less(t, delta, tt.err, "%d/%d/%.2f\nleft %s\nright %s", res.RowCount(), exp, delta, sql.Histogram(xHist).DebugString(), sql.Histogram(wHist).DebugString())
		})
	}
}

func testHistogram(ctx *sql.Context, table *plan.ResolvedTable, fields []int, buckets int) ([]sql.HistogramBucket, error) {
	var cnt uint64
	if st, ok := table.UnderlyingTable().(sql.StatisticsTable); ok {
		var err error
		cnt, _, err = st.RowCount(ctx)
		if err != nil {
			return nil, err
		}
	}
	if cnt == 0 {
		return nil, fmt.Errorf("found zero row count for table")
	}

	i, err := rowexec.DefaultBuilder.Build(ctx, table, nil)
	rows, err := sql.RowIterToRows(ctx, i)
	if err != nil {
		return nil, err
	}

	sch := table.Schema()

	keyVals := make([]sql.Row, len(rows))
	for i, row := range rows {
		for _, ord := range fields {
			keyVals[i] = append(keyVals[i], row[ord])
		}
	}

	cmp := func(i, j int) int {
		k := 0
		for k < len(fields) && keyVals[i][k] == keyVals[j][k] {
			k++
		}
		if k == len(fields) {
			return 0
		}
		col := sch[fields[k]]
		cmp, _ := col.Type.Compare(keyVals[i][k], keyVals[j][k])
		return cmp
	}

	sort.Slice(keyVals, func(i, j int) bool { return cmp(i, j) <= 0 })

	var types []sql.Type
	for _, i := range fields {
		types = append(types, sch[i].Type)
	}

	var histogram []sql.HistogramBucket
	rowsPerBucket := int(cnt) / buckets
	currentBucket := &stats.Bucket{DistinctCnt: 1}
	mcvCnt := 3
	currentCnt := 0
	mcvs := stats.NewSqlHeap(mcvCnt)
	for i, row := range keyVals {
		currentCnt++
		currentBucket.RowCnt++
		if i > 0 {
			if cmp(i, i-1) != 0 {
				currentBucket.DistinctCnt++
				heap.Push(mcvs, stats.NewHeapRow(keyVals[i-1], currentCnt))
				currentCnt = 1
			}
		}
		if currentBucket.RowCnt > uint64(rowsPerBucket) {
			currentBucket.BoundVal = row
			currentBucket.BoundCnt = uint64(currentCnt)
			heap.Push(mcvs, stats.NewHeapRow(row, currentCnt))
			currentBucket.McvVals = mcvs.Array()
			currentBucket.McvsCnt = mcvs.Counts()
			histogram = append(histogram, currentBucket)
			currentBucket = &stats.Bucket{DistinctCnt: 1}
			mcvs = stats.NewSqlHeap(mcvCnt)
			currentCnt = 0
		}
	}
	currentBucket.BoundVal = keyVals[len(keyVals)-1]
	currentBucket.BoundCnt = uint64(currentCnt)
	currentBucket.McvVals = mcvs.Array()
	currentBucket.McvsCnt = mcvs.Counts()
	histogram = append(histogram, currentBucket)
	return histogram, nil
}

func eq(idx1, idx2 int) *expression.Equals {
	return expression.NewEquals(
		expression.NewGetField(idx1, types.Int64, "", false),
		expression.NewGetField(idx2, types.Int64, "", false))
}

func childSchema(source string) sql.PrimaryKeySchema {
	return sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "x", Source: source, Type: types.Int64, Nullable: false},
		{Name: "y", Source: source, Type: types.Int64, Nullable: true},
		{Name: "z", Source: source, Type: types.Int64, Nullable: true},
	}, 0)
}

func makeTable(db *memory.Database, name string, tabId sql.TableId, colId sql.ColumnId) *plan.ResolvedTable {
	t := memory.NewTable(db, name, childSchema(name), nil)
	t.EnablePrimaryKeyIndexes()
	colset := sql.NewColSet(sql.ColumnId(colId), sql.ColumnId(colId+1), sql.ColumnId(colId+2))
	return plan.NewResolvedTable(t, db, nil).WithId(sql.TableId(tabId)).WithColumns(colset).(*plan.ResolvedTable)
}

func newContext(provider *memory.DbProvider) *sql.Context {
	return sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), provider)))
}

func expectedResultSize(ctx *sql.Context, t1, t2 *plan.ResolvedTable, filters []sql.Expression, debug bool) (int, error) {
	j := plan.NewJoin(t1, t2, plan.JoinTypeInner, expression.JoinAnd(filters...))
	if debug {
		fmt.Println(sql.DebugString(j))
	}
	i, err := rowexec.DefaultBuilder.Build(ctx, j, nil)
	if err != nil {
		return 0, err
	}
	cnt := 0
	for {
		_, err := i.Next(ctx)
		if err == io.EOF {
			break
		}

		if err != nil {
			i.Close(ctx)
			return 0, err
		}
		cnt++
	}
	return cnt, nil
}

func uniformDistForTable(ctx *sql.Context, rt *plan.ResolvedTable, cnt int) error {
	tab := rt.UnderlyingTable().(*memory.Table)
	for i := 0; i < cnt; i++ {
		row := sql.Row{int64(i), int64(i), int64(i)}
		err := tab.Insert(ctx, row)
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO sample from exponential distribution
func increasingHalfDistForTable(ctx *sql.Context, rt *plan.ResolvedTable, cnt int) error {
	tab := rt.UnderlyingTable().(*memory.Table)
	for i := 0; i < 2*cnt; i++ {
		for j := 0; j < (j/2)+1; j++ {
			row := sql.Row{int64(i), int64(j), int64(j)}
			err := tab.Insert(ctx, row)
			if err != nil {
				return err
			}
			i++
		}
	}
	return nil
}

func expDistForTable(ctx *sql.Context, rt *plan.ResolvedTable, cnt int, lambda float64) error {
	tab := rt.UnderlyingTable().(*memory.Table)
	iter := stats.NewExpDistIter(2, cnt, lambda)
	var i int
	for {
		val, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		row := sql.Row{int64(val[0].(int))}
		for _, v := range val[1:] {
			row = append(row, int64(v.(float64)))
		}
		err = tab.Insert(ctx, row)
		if err != nil {
			return err
		}
		i++
	}
	return nil
}

func normalDistForTable(ctx *sql.Context, rt *plan.ResolvedTable, cnt int, mean, std float64) error {
	tab := rt.UnderlyingTable().(*memory.Table)
	iter := stats.NewNormDistIter(2, cnt, mean, std)
	var i int
	for {
		val, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		row := sql.Row{int64(i)}
		for _, v := range val[1:] {
			row = append(row, int64(v.(float64)))
		}
		err = tab.Insert(ctx, row)
		if err != nil {
			return err
		}
		i++
	}
	return nil
}
