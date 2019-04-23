package function

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

const (
	tsDate     = 1258882545 // Sunday, November 22, 2009 10:35:45 PM GMT+01:00
	stringDate = "2007-01-02 14:15:16"
)

func TestTime_Year(t *testing.T) {
	f := NewYear(expression.NewGetField(0, sql.Text, "foo", false))
	ctx := sql.NewEmptyContext()

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, false},
		{"date as string", sql.NewRow(stringDate), int32(2007), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Year()), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(2009), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_Month(t *testing.T) {
	f := NewMonth(expression.NewGetField(0, sql.Text, "foo", false))
	ctx := sql.NewEmptyContext()

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, false},
		{"date as string", sql.NewRow(stringDate), int32(1), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Month()), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(11), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_Day(t *testing.T) {
	f := NewDay(expression.NewGetField(0, sql.Text, "foo", false))
	ctx := sql.NewEmptyContext()

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, false},
		{"date as string", sql.NewRow(stringDate), int32(2), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Day()), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(22), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_Weekday(t *testing.T) {
	f := NewWeekday(expression.NewGetField(0, sql.Text, "foo", false))
	ctx := sql.NewEmptyContext()

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, false},
		{"date as string", sql.NewRow(stringDate), int32(1), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Weekday()+6) % 7, false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(6), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_Hour(t *testing.T) {
	f := NewHour(expression.NewGetField(0, sql.Text, "foo", false))
	ctx := sql.NewEmptyContext()

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, false},
		{"date as string", sql.NewRow(stringDate), int32(14), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Hour()), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(9), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_Minute(t *testing.T) {
	f := NewMinute(expression.NewGetField(0, sql.Text, "foo", false))
	ctx := sql.NewEmptyContext()

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, false},
		{"date as string", sql.NewRow(stringDate), int32(15), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Minute()), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(35), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_Second(t *testing.T) {
	f := NewSecond(expression.NewGetField(0, sql.Text, "foo", false))
	ctx := sql.NewEmptyContext()
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, false},
		{"date as string", sql.NewRow(stringDate), int32(16), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Second()), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(45), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_DayOfWeek(t *testing.T) {
	f := NewDayOfWeek(expression.NewGetField(0, sql.Text, "foo", false))
	ctx := sql.NewEmptyContext()

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, false},
		{"date as string", sql.NewRow(stringDate), int32(3), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Weekday() + 1), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(1), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_DayOfYear(t *testing.T) {
	f := NewDayOfYear(expression.NewGetField(0, sql.Text, "foo", false))
	ctx := sql.NewEmptyContext()

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, false},
		{"date as string", sql.NewRow(stringDate), int32(2), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().YearDay()), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(326), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestYearWeek(t *testing.T) {
	f, err := NewYearWeek(expression.NewGetField(0, sql.Text, "foo", false))
	require.NoError(t, err)
	ctx := sql.NewEmptyContext()

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, true},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, true},
		{"date as string", sql.NewRow(stringDate), int32(200653), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(200947), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestCalcDaynr(t *testing.T) {
	require.EqualValues(t, calcDaynr(0, 0, 0), 0)
	require.EqualValues(t, calcDaynr(9999, 12, 31), 3652424)
	require.EqualValues(t, calcDaynr(1970, 1, 1), 719528)
	require.EqualValues(t, calcDaynr(2006, 12, 16), 733026)
	require.EqualValues(t, calcDaynr(10, 1, 2), 3654)
	require.EqualValues(t, calcDaynr(2008, 2, 20), 733457)
}

func TestCalcWeek(t *testing.T) {
	_, w := calcWeek(2008, 2, 20, weekMode(0))

	_, w = calcWeek(2008, 2, 20, weekMode(1))
	require.EqualValues(t, w, 8)

	_, w = calcWeek(2008, 12, 31, weekMode(1))
	require.EqualValues(t, w, 53)
}

func TestNow(t *testing.T) {
	require := require.New(t)
	date := time.Date(2018, time.December, 2, 16, 25, 0, 0, time.Local)
	cl := clock(func() time.Time {
		return date
	})
	f := &Now{cl}

	result, err := f.Eval(nil, nil)
	require.NoError(err)
	require.Equal(date, result)
}

func TestDate(t *testing.T) {
	f := NewDate(expression.NewGetField(0, sql.Text, "foo", false))
	ctx := sql.NewEmptyContext()

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, false},
		{"date as string", sql.NewRow(stringDate), "2007-01-02", false},
		{"date as time", sql.NewRow(time.Now()), time.Now().Format("2006-01-02"), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), "2009-11-22", false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}
