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

var xy1Stat = &Statistic{Hist: buckets_xy_1, Typs: []sql.Type{types.Int64, types.Int64}, fds: xyFds, colSet: sql.NewColSet(1, 2)}

var buckets_xy_1 = []*Bucket{
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, nil}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, nil}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, 1}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, 1}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, 4}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, 4}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{1, nil}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{1, nil}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 0, BoundVal: []interface{}{1, 1}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 0, BoundVal: []interface{}{1, 1}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 0, BoundVal: []interface{}{1, 3}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 0, BoundVal: []interface{}{3, 3}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 0, BoundVal: []interface{}{3, 3}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 0, BoundVal: []interface{}{4, 3}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 0, BoundVal: []interface{}{4, 3}, BoundCnt: 5},
}

var xy2Stat = &Statistic{Hist: buckets_xy_2, Typs: []sql.Type{types.Int64, types.Int64}, fds: xyFds, colSet: sql.NewColSet(1, 2)}

var buckets_xy_2 = []*Bucket{
	{RowCnt: 5, DistinctCnt: 1, NullCnt: 5, BoundVal: []interface{}{nil, nil}, BoundCnt: 5},
	{RowCnt: 5, DistinctCnt: 2, NullCnt: 5, BoundVal: []interface{}{nil, 1}, BoundCnt: 2},
	{RowCnt: 5, DistinctCnt: 2, NullCnt: 5, BoundVal: []interface{}{nil, 4}, BoundCnt: 2},
	{RowCnt: 5, DistinctCnt: 2, NullCnt: 5, BoundVal: []interface{}{1, nil}, BoundCnt: 2},
	{RowCnt: 5, DistinctCnt: 2, NullCnt: 0, BoundVal: []interface{}{1, 1}, BoundCnt: 2},
	{RowCnt: 5, DistinctCnt: 2, NullCnt: 0, BoundVal: []interface{}{1, 3}, BoundCnt: 2},
	{RowCnt: 5, DistinctCnt: 2, NullCnt: 0, BoundVal: []interface{}{2, 3}, BoundCnt: 2},
	{RowCnt: 5, DistinctCnt: 2, NullCnt: 0, BoundVal: []interface{}{3, 3}, BoundCnt: 2},
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
			expBuckets:       7,
			expRowCount:      uint64(35),
			expDistinctCount: uint64(7),
			expNullCount:     uint64(35),
		},
		{
			name:             "xy_1 (1) key",
			statistic:        xy1Stat,
			pref:             []interface{}{1},
			expBuckets:       6,
			expRowCount:      uint64(30),
			expDistinctCount: uint64(6),
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
			name:             "xy_1 (4) key",
			statistic:        xy1Stat,
			pref:             []interface{}{4},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (6) key",
			statistic:        xy1Stat,
			pref:             []interface{}{5},
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
			expNullCount:     uint64(15),
		},
		{
			name:             "xy_1 (nil,1) key",
			statistic:        xy1Stat,
			pref:             []interface{}{nil, 1},
			expBuckets:       3,
			expRowCount:      uint64(15),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(15),
		},
		{
			name:             "xy_1 (nil,2) key",
			statistic:        xy1Stat,
			pref:             []interface{}{nil, 2},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(5),
		},
		{
			name:             "xy_1 (0,nil) key",
			statistic:        xy1Stat,
			pref:             []interface{}{0, nil},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(5),
		},
		{
			name:             "xy_1 (1,nil) key",
			statistic:        xy1Stat,
			pref:             []interface{}{1, nil},
			expBuckets:       3,
			expRowCount:      uint64(15),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(10),
		},
		{
			name:             "xy_1 (1,1) key",
			statistic:        xy1Stat,
			pref:             []interface{}{1, 1},
			expBuckets:       3,
			expRowCount:      uint64(15),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (1,2) key",
			statistic:        xy1Stat,
			pref:             []interface{}{1, 2},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (1,3) key",
			statistic:        xy1Stat,
			pref:             []interface{}{1, 3},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (2,nil) key",
			statistic:        xy1Stat,
			pref:             []interface{}{2, nil},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (2,2) key",
			statistic:        xy1Stat,
			pref:             []interface{}{2, 2},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (2,3) key",
			statistic:        xy1Stat,
			pref:             []interface{}{2, 3},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (3,3) key",
			statistic:        xy1Stat,
			pref:             []interface{}{3, 3},
			expBuckets:       3,
			expRowCount:      uint64(15),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (3,4) key",
			statistic:        xy1Stat,
			pref:             []interface{}{3, 4},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(1),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (4,3) key",
			statistic:        xy1Stat,
			pref:             []interface{}{4, 3},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_1 (4,4) key",
			statistic:        xy1Stat,
			pref:             []interface{}{4, 4},
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
			expBuckets:       4,
			expRowCount:      uint64(20),
			expDistinctCount: uint64(7),
			expNullCount:     uint64(20),
		},
		{
			name:             "xy_2 (2) key",
			statistic:        xy2Stat,
			pref:             []interface{}{2},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(4),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (3) key",
			statistic:        xy2Stat,
			pref:             []interface{}{3},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (5) key",
			statistic:        xy2Stat,
			pref:             []interface{}{5},
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
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(3),
			expNullCount:     uint64(10),
		},
		{
			name:             "xy_2 (nil,1) key",
			statistic:        xy2Stat,
			pref:             []interface{}{nil, 1},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(4),
			expNullCount:     uint64(10),
		},
		{
			name:             "xy_2 (nil,2) key",
			statistic:        xy2Stat,
			pref:             []interface{}{nil, 2},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(5),
		},
		{
			name:             "xy_2 (0,nil) key",
			statistic:        xy2Stat,
			pref:             []interface{}{0, nil},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(5),
		},
		{
			name:             "xy_2 (1,nil) key",
			statistic:        xy2Stat,
			pref:             []interface{}{1, nil},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(4),
			expNullCount:     uint64(5),
		},
		{
			name:             "xy_2 (1,1) key",
			statistic:        xy2Stat,
			pref:             []interface{}{1, 1},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(4),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (1,2) key",
			statistic:        xy2Stat,
			pref:             []interface{}{1, 2},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (1,3) key",
			statistic:        xy2Stat,
			pref:             []interface{}{1, 3},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(4),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (2,nil) key",
			statistic:        xy2Stat,
			pref:             []interface{}{2, nil},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (2,2) key",
			statistic:        xy2Stat,
			pref:             []interface{}{2, 2},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(2),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (2,3) key",
			statistic:        xy2Stat,
			pref:             []interface{}{2, 3},
			expBuckets:       2,
			expRowCount:      uint64(10),
			expDistinctCount: uint64(4),
			expNullCount:     uint64(0),
		},
		{
			name:             "xy_2 (3,3) key",
			statistic:        xy2Stat,
			pref:             []interface{}{3, 3},
			expBuckets:       1,
			expRowCount:      uint64(5),
			expDistinctCount: uint64(2),
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

func collectBounds(s *Statistic) [][]interface{} {
	var bounds [][]interface{}
	for _, b := range s.Histogram() {
		bounds = append(bounds, b.UpperBound())
	}
	return bounds
}

// TODO these will use the same tests as above, just with different expected values
func TestPrefixIsNull(t *testing.T) {
	tests := []struct {
		vals     [][]interface{}
		key      []interface{}
		expLower int
		expUpper int
		typs     []sql.Type
	}{
		{
			vals:     [][]interface{}{{nil}, {2}, {3}, {4}},
			expLower: 0,
			expUpper: 1,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {nil}, {nil}, {2}, {3}, {4}},
			expLower: 0,
			expUpper: 3,
			typs:     []sql.Type{types.Int64},
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("is null bound: %#v", tt.vals), func(t *testing.T) {
			cols := sql.NewFastIntSet()
			cols.AddRange(1, len(tt.typs))
			colset := sql.NewColSetFromIntSet(cols)
			fds := sql.NewTablescanFDs(colset, []sql.ColSet{colset}, nil, colset)
			var buckets []*Bucket
			for _, v := range tt.vals {
				buckets = append(buckets, &Bucket{BoundVal: v})
			}

			statistic := &Statistic{Hist: buckets, Typs: tt.typs, fds: fds, colSet: colset}

			res, err := PrefixIsNull(statistic)
			require.NoError(t, err)
			bounds := collectBounds(res.(*Statistic))
			require.ElementsMatch(t, tt.vals[tt.expLower:tt.expUpper], bounds)
		})
	}
}

func TestPrefixIsNotNull(t *testing.T) {
	tests := []struct {
		vals     [][]interface{}
		key      []interface{}
		expLower int
		expUpper int
		typs     []sql.Type
	}{
		{
			vals:     [][]interface{}{{nil}, {2}, {3}, {4}},
			expLower: 1,
			expUpper: 4,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {nil}, {nil}, {2}, {3}, {4}},
			expLower: 3,
			expUpper: 6,
			typs:     []sql.Type{types.Int64},
		},
	}
	for _, tt := range tests {
		cols := sql.NewFastIntSet()
		cols.AddRange(1, len(tt.typs))
		colset := sql.NewColSetFromIntSet(cols)
		fds := sql.NewTablescanFDs(colset, []sql.ColSet{colset}, nil, colset)

		var buckets []*Bucket
		for _, v := range tt.vals {
			buckets = append(buckets, &Bucket{BoundVal: v})
		}

		statistic := &Statistic{Hist: buckets, Typs: tt.typs, fds: fds, colSet: colset}

		t.Run(fmt.Sprintf("is not null bound: %#v", tt.vals), func(t *testing.T) {
			res, err := PrefixIsNotNull(statistic)
			require.NoError(t, err)
			bounds := collectBounds(res.(*Statistic))
			require.ElementsMatch(t, tt.vals[tt.expLower:tt.expUpper], bounds)
		})

	}
}

func TestPrefixGt(t *testing.T) {
	tests := []struct {
		vals     [][]interface{}
		key      interface{}
		expLower int
		expUpper int
		typs     []sql.Type
	}{
		{
			vals:     [][]interface{}{{nil}, {2}, {3}, {4}},
			key:      2,
			expLower: 2,
			expUpper: 4,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {nil}, {nil}, {2}, {3}, {4}},
			key:      nil,
			expLower: 3,
			expUpper: 6,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {nil}, {nil}, {2}, {3}, {4}},
			key:      3,
			expLower: 5,
			expUpper: 6,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {nil}, {nil}, {2}, {3}, {4}},
			key:      4,
			expLower: 6,
			expUpper: 6,
			typs:     []sql.Type{types.Int64},
		},
	}
	for _, tt := range tests {
		cols := sql.NewFastIntSet()
		cols.AddRange(1, len(tt.typs))
		colset := sql.NewColSetFromIntSet(cols)
		fds := sql.NewTablescanFDs(colset, []sql.ColSet{colset}, nil, colset)

		var buckets []*Bucket
		for _, v := range tt.vals {
			buckets = append(buckets, &Bucket{BoundVal: v})
		}

		statistic := &Statistic{Hist: buckets, Typs: tt.typs, fds: fds, colSet: colset}

		t.Run(fmt.Sprintf("GT bound: %d", tt.key), func(t *testing.T) {
			res, err := PrefixGt(statistic, tt.key)
			require.NoError(t, err)
			bounds := collectBounds(res.(*Statistic))
			require.ElementsMatch(t, tt.vals[tt.expLower:tt.expUpper], bounds)
		})
	}
}

func TestPrefixGte(t *testing.T) {
	tests := []struct {
		vals     [][]interface{}
		key      interface{}
		expLower int
		expUpper int
		typs     []sql.Type
	}{
		{
			vals:     [][]interface{}{{nil}, {2}, {3}, {4}},
			key:      2,
			expLower: 1,
			expUpper: 4,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {nil}, {nil}, {2}, {3}, {4}},
			key:      3,
			expLower: 4,
			expUpper: 6,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {nil}, {nil}, {2}, {3}, {4}},
			key:      4,
			expLower: 5,
			expUpper: 6,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {nil}, {nil}, {2}, {3}, {4}},
			key:      5,
			expLower: 6,
			expUpper: 6,
			typs:     []sql.Type{types.Int64},
		},
	}
	for _, tt := range tests {
		cols := sql.NewFastIntSet()
		cols.AddRange(1, len(tt.typs))
		colset := sql.NewColSetFromIntSet(cols)
		fds := sql.NewTablescanFDs(colset, []sql.ColSet{colset}, nil, colset)

		var buckets []*Bucket
		for _, v := range tt.vals {
			buckets = append(buckets, &Bucket{BoundVal: v})
		}

		statistic := &Statistic{Hist: buckets, Typs: tt.typs, fds: fds, colSet: colset}

		t.Run(fmt.Sprintf("GTE bound: %v", tt.key), func(t *testing.T) {
			res, err := PrefixGte(statistic, tt.key)
			require.NoError(t, err)
			bounds := collectBounds(res.(*Statistic))
			require.ElementsMatch(t, tt.vals[tt.expLower:tt.expUpper], bounds)
		})
	}
}

func TestPrefixLt(t *testing.T) {
	tests := []struct {
		vals     [][]interface{}
		key      interface{}
		expLower int
		expUpper int
		typs     []sql.Type
	}{
		{
			vals:     [][]interface{}{{nil}, {2}, {3}, {4}},
			key:      2,
			expLower: 1,
			expUpper: 1,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {2}, {3}, {4}},
			key:      nil,
			expLower: 1,
			expUpper: 1,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{2}, {3}, {4}},
			key:      2,
			expLower: 0,
			expUpper: 0,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {nil}, {nil}, {2}, {3}, {4}},
			key:      3,
			expLower: 3,
			expUpper: 4,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {nil}, {nil}, {2}, {3}, {4}},
			key:      4,
			expLower: 3,
			expUpper: 5,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {nil}, {nil}, {2}, {3}, {4}},
			key:      5,
			expLower: 3,
			expUpper: 6,
			typs:     []sql.Type{types.Int64},
		},
	}
	for _, tt := range tests {
		cols := sql.NewFastIntSet()
		cols.AddRange(1, len(tt.typs))
		colset := sql.NewColSetFromIntSet(cols)
		fds := sql.NewTablescanFDs(colset, []sql.ColSet{colset}, nil, colset)

		var buckets []*Bucket
		for _, v := range tt.vals {
			buckets = append(buckets, &Bucket{BoundVal: v})
		}

		statistic := &Statistic{Hist: buckets, Typs: tt.typs, fds: fds, colSet: colset}

		t.Run(fmt.Sprintf("LT bound: %v", tt.key), func(t *testing.T) {

			res, err := PrefixLt(statistic, tt.key)
			require.NoError(t, err)
			bounds := collectBounds(res.(*Statistic))
			require.ElementsMatch(t, tt.vals[tt.expLower:tt.expUpper], bounds)
		})
	}
}

func TestPrefixLte(t *testing.T) {
	tests := []struct {
		vals     [][]interface{}
		key      interface{}
		expLower int
		expUpper int
		typs     []sql.Type
	}{
		{
			vals:     [][]interface{}{{nil}, {2}, {3}, {4}},
			key:      2,
			expLower: 1,
			expUpper: 2,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {2}, {3}, {4}},
			key:      nil,
			expLower: 1,
			expUpper: 1,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{2}, {3}, {4}},
			key:      2,
			expLower: 0,
			expUpper: 1,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {nil}, {nil}, {2}, {3}, {4}},
			key:      3,
			expLower: 3,
			expUpper: 5,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {nil}, {nil}, {2}, {3}, {4}},
			key:      4,
			expLower: 3,
			expUpper: 6,
			typs:     []sql.Type{types.Int64},
		},
		{
			vals:     [][]interface{}{{nil}, {nil}, {nil}, {2}, {3}, {4}},
			key:      5,
			expLower: 3,
			expUpper: 6,
			typs:     []sql.Type{types.Int64},
		},
	}
	for _, tt := range tests {
		cols := sql.NewFastIntSet()
		cols.AddRange(1, len(tt.typs))
		colset := sql.NewColSetFromIntSet(cols)
		fds := sql.NewTablescanFDs(colset, []sql.ColSet{colset}, nil, colset)

		var buckets []*Bucket
		for _, v := range tt.vals {
			buckets = append(buckets, &Bucket{BoundVal: v})
		}

		statistic := &Statistic{Hist: buckets, Typs: tt.typs, fds: fds, colSet: colset}

		t.Run(fmt.Sprintf("LTE bound: %v", tt.key), func(t *testing.T) {
			res, err := PrefixLte(statistic, tt.key)
			require.NoError(t, err)
			bounds := collectBounds(res.(*Statistic))
			require.ElementsMatch(t, tt.vals[tt.expLower:tt.expUpper], bounds)
		})
	}
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
