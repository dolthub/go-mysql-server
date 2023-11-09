package stats

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
	"testing"
)

var xFds = sql.NewTablescanFDs(sql.NewColSet(1, 2, 3), []sql.ColSet{sql.NewColSet(1)}, nil, sql.NewColSet(1, 2, 3))

// NULL, 0,5,10,15,20, 5 row buckets of duplicates, two buckets for each value
var x1Stat = &Statistic{Hist: buckets_x_1, Typs: []sql.Type{types.Int64}, fds: xFds, colSet: sql.NewColSet(1)}
var buckets_x_1 = []*Bucket{
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 0, BoundVal: []interface{}{5}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 0, BoundVal: []interface{}{5}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 0, BoundVal: []interface{}{10}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 0, BoundVal: []interface{}{10}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 0, BoundVal: []interface{}{15}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 0, BoundVal: []interface{}{15}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 0, BoundVal: []interface{}{20}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 0, BoundVal: []interface{}{20}, BoundCnt: 5},
}

// staggered buckets, 2 vals per, half is last bound half is current bound
var x2Stat = &Statistic{Hist: buckets_x_2, Typs: []sql.Type{types.Int64}, fds: xFds, colSet: sql.NewColSet(1)}
var buckets_x_2 = []*Bucket{
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 2, NullCnt: 3, BoundVal: []interface{}{5}, BoundCnt: 2},
	{RowCnt: 5, DistinctCnt: 2, NullCnt: 0, BoundVal: []interface{}{10}, BoundCnt: 2},
	{RowCnt: 5, DistinctCnt: 2, NullCnt: 0, BoundVal: []interface{}{15}, BoundCnt: 2},
	{RowCnt: 5, DistinctCnt: 2, NullCnt: 0, BoundVal: []interface{}{20}, BoundCnt: 2},
}

var xyFds = sql.NewTablescanFDs(sql.NewColSet(1, 2, 3), []sql.ColSet{sql.NewColSet(1, 2)}, nil, sql.NewColSet(1, 2, 3))

var xy1Stat = &Statistic{Hist: buckets_x_1, Typs: []sql.Type{types.Int64}, fds: xFds, colSet: sql.NewColSet(1)}

var buckets_xy_1 = []*Bucket{
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, nil}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, nil}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, 1}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, 1}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, 4}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, 4}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{1, nil}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{1, nil}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{1, 1}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{1, 1}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{1, 3}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{2, 3}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{2, 3}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{3, 3}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{3, 3}, BoundCnt: 5},
}

var xy2Stat = &Statistic{Hist: buckets_x_1, Typs: []sql.Type{types.Int64, types.Int64}, fds: xyFds, colSet: sql.NewColSet(1, 2)}

// TODO vals cross boundaries
var buckets_xy_2 = []*Bucket{
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, nil}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, 1}, BoundCnt: 2},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, 4}, BoundCnt: 2},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{1, nil}, BoundCnt: 2},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{1, 1}, BoundCnt: 2},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{1, 3}, BoundCnt: 2},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{2, 3}, BoundCnt: 2},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{3, 3}, BoundCnt: 2},
}

func TestPrefixKey(t *testing.T) {
	tests := []struct {
		name             string
		statistic        *Statistic
		pref             []interface{}
		expBuckets       int
		expRowCount      uint64
		expDistinctCount uint64
		expNullCount     uint64
	}{
		{
			name:             "x_1 nil key",
			statistic:        x1Stat,
			pref:             []interface{}{nil},
			expBuckets:       3,
			expRowCount:      uint64(15),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(10),
		},
		{
			name:             "x_1 (2) key",
			statistic:        x1Stat,
			pref:             []interface{}{2},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(0),
		},
		{
			name:             "x_1 (5) key",
			statistic:        x1Stat,
			pref:             []interface{}{5},
			expBuckets:       3,
			expRowCount:      uint64(15),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(0),
		},
		{
			name:             "x_1 (6) key",
			statistic:        x1Stat,
			pref:             []interface{}{6},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(0),
		},
		{
			name:             "x_1 (20) key",
			statistic:        x1Stat,
			pref:             []interface{}{20},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(0),
		},
		{
			name:             "x_1 (21) key",
			statistic:        x1Stat,
			pref:             []interface{}{21},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		// x2
		{
			name:             "x_2 (nil) key",
			statistic:        x2Stat,
			pref:             []interface{}{nil},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(8),
		},
		{
			name:             "x_2 (2) key",
			statistic:        x2Stat,
			pref:             []interface{}{2},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(3),
		},
		{
			name:             "x_2 (5) key",
			statistic:        x2Stat,
			pref:             []interface{}{5},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(4),
			expNullCount:     uint64(3),
		},
		{
			name:             "x_2 (6) key",
			statistic:        x2Stat,
			pref:             []interface{}{6},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(0),
		},
		{
			name:             "x_2 (20) key",
			statistic:        x2Stat,
			pref:             []interface{}{20},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(0),
		},
		{
			name:             "x_2 (21) key",
			statistic:        x2Stat,
			pref:             []interface{}{21},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		// xy1
		{
			name:             "xy_1 nil key",
			statistic:        xy1Stat,
			pref:             []interface{}{nil},
			expBuckets:       3,
			expRowCount:      uint64(15),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(10),
		},
		{
			name:             "xy_1 (2) key",
			statistic:        xy1Stat,
			pref:             []interface{}{2},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (5) key",
			statistic:        xy1Stat,
			pref:             []interface{}{5},
			expBuckets:       3,
			expRowCount:      uint64(15),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (6) key",
			statistic:        xy1Stat,
			pref:             []interface{}{6},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (20) key",
			statistic:        xy1Stat,
			pref:             []interface{}{20},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (21) key",
			statistic:        xy1Stat,
			pref:             []interface{}{21},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		// 2-values xy1
		{
			name:             "xy_1 (nil,nil) key",
			statistic:        xy1Stat,
			pref:             []interface{}{nil, nil},
			expBuckets:       3,
			expRowCount:      uint64(15),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(10),
		},
		{
			name:             "xy_1 (nil,1) key",
			statistic:        xy1Stat,
			pref:             []interface{}{nil, 1},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (nil,2) key",
			statistic:        xy1Stat,
			pref:             []interface{}{nil, 2},
			expBuckets:       3,
			expRowCount:      uint64(15),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (0,nil) key",
			statistic:        xy1Stat,
			pref:             []interface{}{0, nil},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (1,nil) key",
			statistic:        xy1Stat,
			pref:             []interface{}{1, nil},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (1,1) key",
			statistic:        xy1Stat,
			pref:             []interface{}{1, 1},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (1,2) key",
			statistic:        xy1Stat,
			pref:             []interface{}{1, 2},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (1,3) key",
			statistic:        xy1Stat,
			pref:             []interface{}{1, 3},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (2,nil) key",
			statistic:        xy1Stat,
			pref:             []interface{}{2, nil},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (2,2) key",
			statistic:        xy1Stat,
			pref:             []interface{}{2, 2},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (2,3) key",
			statistic:        xy1Stat,
			pref:             []interface{}{2, 3},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (3,3) key",
			statistic:        xy1Stat,
			pref:             []interface{}{3, 3},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (3,4) key",
			statistic:        xy1Stat,
			pref:             []interface{}{3, 4},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (4,3) key",
			statistic:        xy1Stat,
			pref:             []interface{}{4, 3},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		// xy2
		{
			name:             "xy_2 nil key",
			statistic:        xy2Stat,
			pref:             []interface{}{nil},
			expBuckets:       3,
			expRowCount:      uint64(15),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(10),
		},
		{
			name:             "xy_2 (2) key",
			statistic:        xy2Stat,
			pref:             []interface{}{2},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (5) key",
			statistic:        xy2Stat,
			pref:             []interface{}{5},
			expBuckets:       3,
			expRowCount:      uint64(15),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (6) key",
			statistic:        xy2Stat,
			pref:             []interface{}{6},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (20) key",
			statistic:        xy2Stat,
			pref:             []interface{}{20},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (21) key",
			statistic:        xy2Stat,
			pref:             []interface{}{21},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		// 2-values xy1
		{
			name:             "xy_2 (nil,nil) key",
			statistic:        xy2Stat,
			pref:             []interface{}{nil, nil},
			expBuckets:       3,
			expRowCount:      uint64(15),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(10),
		},
		{
			name:             "xy_2 (nil,1) key",
			statistic:        xy2Stat,
			pref:             []interface{}{nil, 1},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (nil,2) key",
			statistic:        xy2Stat,
			pref:             []interface{}{nil, 2},
			expBuckets:       3,
			expRowCount:      uint64(15),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (0,nil) key",
			statistic:        xy2Stat,
			pref:             []interface{}{0, nil},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (1,nil) key",
			statistic:        xy2Stat,
			pref:             []interface{}{1, nil},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (1,1) key",
			statistic:        xy2Stat,
			pref:             []interface{}{1, 1},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (1,2) key",
			statistic:        xy2Stat,
			pref:             []interface{}{1, 2},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (1,3) key",
			statistic:        xy2Stat,
			pref:             []interface{}{1, 3},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (2,nil) key",
			statistic:        xy2Stat,
			pref:             []interface{}{2, nil},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (2,2) key",
			statistic:        xy2Stat,
			pref:             []interface{}{2, 2},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (2,3) key",
			statistic:        xy2Stat,
			pref:             []interface{}{2, 3},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (3,3) key",
			statistic:        xy2Stat,
			pref:             []interface{}{3, 3},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (3,4) key",
			statistic:        xy2Stat,
			pref:             []interface{}{3, 4},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (4,3) key",
			statistic:        xy2Stat,
			pref:             []interface{}{4, 3},
			expBuckets:       0,
			expRowCount:      uint64(0),
			expDistinctCount: uint64(0),
			expNullCount:     uint64(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := PrefixKey(tt.statistic, tt.pref, make([]bool, len(tt.pref)))
			require.NoError(t, err)
			require.Equal(t, tt.expBuckets, len(res.Histogram()))
			require.Equal(t, tt.expRowCount, res.RowCount())
			require.Equal(t, tt.expDistinctCount, res.DistinctCount())
			require.Equal(t, tt.expNullCount, res.NullCount())
		})
	}
}

// TODO these will use the same tests as above, just with different expected values
func TestPrefixIsNull(t *testing.T) {

}

func TestPrefixIsNotNull(t *testing.T) {

}

func TestPrefixGt(t *testing.T) {

}

func TestPrefixGte(t *testing.T) {

}

func TestPrefixLt(t *testing.T) {

}

func TestPrefixLte(t *testing.T) {

}

func TestUpdateCounts(t *testing.T) {
	tests := []struct {
		vals []uint64
		exp  uint64
	}{
		{
			vals: []uint64{1, 1, 1, 4, 1},
			exp:  uint64(8),
		},
		{
			vals: []uint64{1000, 1, 1, 4, 1},
			exp:  uint64(1007),
		},
	}

	for _, tt := range tests {
		buckets := make([]*Bucket, len(tt.vals))
		for i := range buckets {
			buckets[i] = &Bucket{RowCnt: tt.vals[i], DistinctCnt: tt.vals[i], NullCnt: tt.vals[i]}
		}
		s := &Statistic{Hist: buckets}
		t.Run(fmt.Sprintf("count update: %#v", tt.vals), func(t *testing.T) {
			ret := UpdateCounts(s)
			require.Equal(t, tt.exp, ret.RowCount())
			require.Equal(t, tt.exp, ret.DistinctCount())
			require.Equal(t, tt.exp, ret.NullCount())
		})
	}
}
