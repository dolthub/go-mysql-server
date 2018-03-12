package expression

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

const (
	tsDate     = 1258882545 // Sunday, November 22, 2009 10:35:45 PM GMT+01:00
	stringDate = "2007-01-02 14:15:16"
)

func TestTime_Year(t *testing.T) {
	f := NewYear(NewGetField(0, sql.Text, "foo", false))
	session := sql.NewBaseSession(context.TODO())

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, true},
		{"date as string", sql.NewRow(stringDate), int32(2007), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Year()), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(2009), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(session, tt.row)
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
	f := NewMonth(NewGetField(0, sql.Text, "foo", false))
	session := sql.NewBaseSession(context.TODO())

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, true},
		{"date as string", sql.NewRow(stringDate), int32(1), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Month()), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(11), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(session, tt.row)
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
	f := NewDay(NewGetField(0, sql.Text, "foo", false))
	session := sql.NewBaseSession(context.TODO())

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, true},
		{"date as string", sql.NewRow(stringDate), int32(2), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Day()), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(22), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(session, tt.row)
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
	f := NewHour(NewGetField(0, sql.Text, "foo", false))
	session := sql.NewBaseSession(context.TODO())

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, true},
		{"date as string", sql.NewRow(stringDate), int32(14), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Hour()), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(9), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(session, tt.row)
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
	f := NewMinute(NewGetField(0, sql.Text, "foo", false))
	session := sql.NewBaseSession(context.TODO())

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, true},
		{"date as string", sql.NewRow(stringDate), int32(15), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Minute()), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(35), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(session, tt.row)
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
	f := NewSecond(NewGetField(0, sql.Text, "foo", false))
	session := sql.NewBaseSession(context.TODO())
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, true},
		{"date as string", sql.NewRow(stringDate), int32(16), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Second()), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(45), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(session, tt.row)
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
	f := NewDayOfYear(NewGetField(0, sql.Text, "foo", false))
	session := sql.NewBaseSession(context.TODO())

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, true},
		{"date as string", sql.NewRow(stringDate), int32(2), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().YearDay()), false},
		{"date as unix timestamp", sql.NewRow(int64(tsDate)), int32(326), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(session, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}
