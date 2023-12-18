package enginetest

import (
	"container/heap"
	"context"
	"fmt"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
	"io"
	"log"
	"math"
	"sort"
	"testing"
)

func init() {
	rand.Seed(0)
}

type statsTest struct {
	name       string
	left       func(*sql.Context, *memory.Database, int, sql.TableId, sql.ColumnId) *plan.ResolvedTable
	right      func(*sql.Context, *memory.Database, int, sql.TableId, sql.ColumnId) *plan.ResolvedTable
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
	}

	var statTests []statsTest
	for _, t := range tests {
		st := statsTest{
			name: t.name,
			left: func(ctx *sql.Context, db *memory.Database, cnt int, tab sql.TableId, col sql.ColumnId) *plan.ResolvedTable {
				xyz := makeTable(db, fmt.Sprintf("xyz%d", tab), tab, col)
				err := normalDistForTable(ctx, xyz, cnt, t.mean1, t.std1, int(tab))
				if err != nil {
					panic(err)
				}
				return xyz
			},
			right: func(ctx *sql.Context, db *memory.Database, cnt int, tab sql.TableId, col sql.ColumnId) *plan.ResolvedTable {
				xyz := makeTable(db, fmt.Sprintf("xyz%d", tab), tab, col)
				err := normalDistForTable(ctx, xyz, cnt, t.mean2, t.std2, int(tab))
				if err != nil {
					panic(err)
				}
				return xyz
			},
			leftOrd:    []int{1},
			rightOrd:   []int{1},
			leftTypes:  []sql.Type{types.Int64, types.Int64, types.Int64},
			rightTypes: []sql.Type{types.Int64, types.Int64, types.Int64},
			err:        .5,
		}
		statTests = append(statTests, st)
	}

	runSuite(t, statTests, 100, 5, true)
	runSuite(t, statTests, 100, 10, true)
	runSuite(t, statTests, 100, 20, true)
	runSuite(t, statTests, 500, 10, true)
	runSuite(t, statTests, 500, 20, true)
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
			name:    ".5/3",
			lambda1: .5,
			lambda2: 3,
		},
	}

	var statTests []statsTest
	for _, t := range tests {
		st := statsTest{
			name: t.name,
			left: func(ctx *sql.Context, db *memory.Database, cnt int, tab sql.TableId, col sql.ColumnId) *plan.ResolvedTable {
				xyz := makeTable(db, "xyz", tab, col)
				err := expDistForTable(ctx, xyz, cnt, t.lambda1, int(tab))
				if err != nil {
					panic(err)
				}
				return xyz
			},
			right: func(ctx *sql.Context, db *memory.Database, cnt int, tab sql.TableId, col sql.ColumnId) *plan.ResolvedTable {
				xyz := makeTable(db, "xyz", tab, col)
				err := expDistForTable(ctx, xyz, cnt, t.lambda2, int(tab))
				if err != nil {
					panic(err)
				}
				return xyz
			},
			leftOrd:    []int{1},
			rightOrd:   []int{1},
			leftTypes:  []sql.Type{types.Int64, types.Int64, types.Int64},
			rightTypes: []sql.Type{types.Int64, types.Int64, types.Int64},
			err:        .5,
		}
		statTests = append(statTests, st)
	}

	runSuite(t, statTests, 100, 5, false)
	runSuite(t, statTests, 100, 10, false)
	runSuite(t, statTests, 100, 20, false)
	runSuite(t, statTests, 500, 10, false)
	runSuite(t, statTests, 500, 20, false)
}

func TestMultiDist(t *testing.T) {
	tests := []statsTest{
		{
			name: "uniform dist int",
			left: func(ctx *sql.Context, db *memory.Database, cnt int, tab sql.TableId, col sql.ColumnId) *plan.ResolvedTable {
				xyz := makeTable(db, "xyz", tab, col)
				err := uniformDistForTable(ctx, xyz, cnt)
				if err != nil {
					panic(err)
				}
				return xyz
			},
			right: func(ctx *sql.Context, db *memory.Database, cnt int, tab sql.TableId, col sql.ColumnId) *plan.ResolvedTable {
				wuv := makeTable(db, "wuv", tab, col)
				err := uniformDistForTable(ctx, wuv, cnt)
				if err != nil {
					panic(err)
				}
				return wuv
			},
			leftOrd:    []int{1},
			rightOrd:   []int{1},
			leftTypes:  []sql.Type{types.Int64, types.Int64, types.Int64},
			rightTypes: []sql.Type{types.Int64, types.Int64, types.Int64},
			err:        .01,
		},
	}

	runSuite(t, tests, 1000, 10, true)
}

func runSuite(t *testing.T, tests []statsTest, rowCnt, bucketCnt int, debug bool) {
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s: , rows: %d, buckets: %d", tt.name, rowCnt, bucketCnt), func(t *testing.T) {
			xyz := tt.left(ctx, db, rowCnt, 1, 1)
			wuv := tt.left(ctx, db, rowCnt, 2, sql.ColumnId(len(tt.leftTypes)+1))

			// join the histograms on a particular field
			var joinOps []sql.Expression
			for i, l := range tt.leftOrd {
				r := tt.rightOrd[i]
				joinOps = append(joinOps, eq(l, r+len(tt.leftTypes)))
			}

			exp, err := expectedResultSize(ctx, xyz, wuv, joinOps)
			require.NoError(t, err)

			// get histograms for tables
			xHist, err := testHistogram(ctx, xyz, tt.leftOrd, bucketCnt)
			require.NoError(t, err)

			wHist, err := testHistogram(ctx, wuv, tt.rightOrd, bucketCnt)
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

			res, err := stats.Join(lStat, rStat, []int{0}, []int{0})
			require.NoError(t, err)
			if debug {
				log.Printf("join:\n,%s\n", res.Histogram().DebugString())
			}

			delta := float64(exp-int(res.RowCount())) / float64(exp)
			if delta < 0 {
				delta = -delta
			}
			if debug {
				log.Println(res.RowCount(), exp, delta)
			}
			require.Less(t, delta, tt.err)
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
	mcvs := stats.NewSqlHeap(types, mcvCnt)
	for i, row := range keyVals {
		currentCnt++
		currentBucket.RowCnt++
		heap.Push(mcvs, row)
		if i > 0 {
			if cmp(i, i-1) != 0 {
				currentBucket.DistinctCnt++
				currentCnt = 1
			}
		}
		if currentBucket.RowCnt > uint64(rowsPerBucket) {
			currentBucket.BoundVal = row
			currentBucket.BoundCnt = uint64(currentCnt)
			histogram = append(histogram, currentBucket)
			currentBucket = &stats.Bucket{DistinctCnt: 1}
			currentCnt = 0
		}
	}
	currentBucket.BoundVal = keyVals[len(keyVals)-1]
	currentBucket.BoundCnt = uint64(currentCnt)
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
	return plan.NewResolvedTable(t, nil, nil).WithId(sql.TableId(tabId)).WithColumns(colset).(*plan.ResolvedTable)
}

func newContext(provider *memory.DbProvider) *sql.Context {
	return sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), provider)))
}

func expectedResultSize(ctx *sql.Context, t1, t2 *plan.ResolvedTable, filters []sql.Expression) (int, error) {
	j := plan.NewJoin(t1, t2, plan.JoinTypeInner, expression.JoinAnd(filters...))
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

func expDistForTable(ctx *sql.Context, rt *plan.ResolvedTable, cnt int, lambda float64, seed int) error {
	rand.Seed(uint64(seed))
	tab := rt.UnderlyingTable().(*memory.Table)
	for i := 0; i < 2*cnt; i++ {
		k := i
		for j := 0; j < k; j++ {
			y := -math.Log2(rand.NormFloat64()) / lambda
			z := -math.Log2(rand.NormFloat64()) / lambda
			row := sql.Row{int64(i), int64(y), int64(z)}
			err := tab.Insert(ctx, row)
			if err != nil {
				return err
			}
			i++
		}
	}
	return nil
}

func normalDistForTable(ctx *sql.Context, rt *plan.ResolvedTable, cnt int, mean, std float64, seed int) error {
	rand.Seed(uint64(seed))
	tab := rt.UnderlyingTable().(*memory.Table)
	for i := 0; i < cnt; i++ {
		y := rand.NormFloat64()*std + mean
		z := rand.NormFloat64()*std + mean
		row := sql.Row{int64(i), int64(y), int64(z)}
		err := tab.Insert(ctx, row)
		if err != nil {
			return err
		}
	}
	return nil
}
