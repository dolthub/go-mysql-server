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
	"sort"
	"testing"
)

func init() {
	rand.Seed(0)
}

func TestUniformDistributionJoin(t *testing.T) {

	tests := []struct {
		name       string
		left       func(*sql.Context, *memory.Database, int) *plan.ResolvedTable
		right      func(*sql.Context, *memory.Database, int) *plan.ResolvedTable
		leftOrd    []int
		rightOrd   []int
		leftTypes  []sql.Type
		rightTypes []sql.Type
		err        float64
	}{
		{
			name: "uniform dist int",
			left: func(ctx *sql.Context, db *memory.Database, cnt int) *plan.ResolvedTable {
				xyz := makeTable(db, "xyz", 1, 1)
				err := uniformDistForTable(ctx, xyz, cnt)
				if err != nil {
					panic(err)
				}
				return xyz
			},
			right: func(ctx *sql.Context, db *memory.Database, cnt int) *plan.ResolvedTable {
				wuv := makeTable(db, "wuv", 2, 4)
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
		{
			name: "normal dist int",
			left: func(ctx *sql.Context, db *memory.Database, cnt int) *plan.ResolvedTable {
				xyz := makeTable(db, "xyz", 1, 1)
				err := normalDistForTable(ctx, xyz, cnt)
				if err != nil {
					panic(err)
				}
				return xyz
			},
			right: func(ctx *sql.Context, db *memory.Database, cnt int) *plan.ResolvedTable {
				wuv := makeTable(db, "wuv", 2, 4)
				err := normalDistForTable(ctx, wuv, cnt)
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
		{
			name: "normal dist int",
			left: func(ctx *sql.Context, db *memory.Database, cnt int) *plan.ResolvedTable {
				xyz := makeTable(db, "xyz", 1, 1)
				err := normalDistForTable(ctx, xyz, cnt)
				if err != nil {
					panic(err)
				}
				return xyz
			},
			right: func(ctx *sql.Context, db *memory.Database, cnt int) *plan.ResolvedTable {
				wuv := makeTable(db, "wuv", 2, 4)
				err := normalDistForTable(ctx, wuv, cnt)
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
		{
			name: "exponential dist int",
			left: func(ctx *sql.Context, db *memory.Database, cnt int) *plan.ResolvedTable {
				xyz := makeTable(db, "xyz", 1, 1)
				err := increasingHalfDistForTable(ctx, xyz, cnt)
				if err != nil {
					panic(err)
				}
				return xyz
			},
			right: func(ctx *sql.Context, db *memory.Database, cnt int) *plan.ResolvedTable {
				wuv := makeTable(db, "wuv", 2, 4)
				err := increasingHalfDistForTable(ctx, wuv, cnt)
				if err != nil {
					panic(err)
				}
				return wuv
			},
			leftOrd:    []int{1},
			rightOrd:   []int{1},
			leftTypes:  []sql.Type{types.Int64, types.Int64, types.Int64},
			rightTypes: []sql.Type{types.Int64, types.Int64, types.Int64},
			err:        3000,
		},
	}

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xyz := tt.left(ctx, db, 1000)
			wuv := tt.left(ctx, db, 1000)

			// join the histograms on a particular field
			exp, err := expectedResultSize(ctx, xyz, wuv, []sql.Expression{eq(1, 4)})
			require.NoError(t, err)

			// get histograms for tables
			xHist, err := testHistogram(ctx, xyz, []int{1}, 10)
			require.NoError(t, err)

			wHist, err := testHistogram(ctx, wuv, []int{1}, 10)
			require.NoError(t, err)

			//log.Println(sql.Histogram(xHist).DebugString())
			//log.Println(sql.Histogram(wHist).DebugString())

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
			//log.Println(res.Histogram().DebugString())

			delta := float64(exp-int(res.RowCount())) / float64(exp)
			if delta < 0 {
				delta = -delta
			}
			log.Println(res.RowCount(), exp, delta)
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

func increasingLinearDistForTable(ctx *sql.Context, rt *plan.ResolvedTable, cnt int) error {
	tab := rt.UnderlyingTable().(*memory.Table)
	for i := 0; i < 2*cnt; i++ {
		k := i
		for j := 0; j < k; j++ {
			row := sql.Row{int64(i), int64(k), int64(k)}
			err := tab.Insert(ctx, row)
			if err != nil {
				return err
			}
			i++
		}
	}
	return nil
}

func normalDistForTable(ctx *sql.Context, rt *plan.ResolvedTable, cnt int) error {
	tab := rt.UnderlyingTable().(*memory.Table)
	for i := 0; i < cnt; i++ {
		y := rand.NormFloat64()*float64(cnt/4) + float64(cnt/2)
		z := rand.NormFloat64()*float64(cnt/4) + float64(cnt/2)
		row := sql.Row{int64(i), int64(y), int64(z)}
		err := tab.Insert(ctx, row)
		if err != nil {
			return err
		}
	}
	return nil
}
