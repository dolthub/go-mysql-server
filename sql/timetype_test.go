package sql

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeCompare(t *testing.T) {
	// This is here so that it doesn't pollute the namespace
	parseDuration := func(str string) time.Duration {
		d, err := time.ParseDuration(str)
		if err != nil {
			panic(err)
		}
		return d
	}

	tests := []struct {
		val1 interface{}
		val2 interface{}
		expectedCmp int
	}{
		{-1, 1, -1},
		{59, -59, 1},
		{parseDuration("100ms"), 1, -1},
		{510144, parseDuration("46h32m"), 1},
		{40.134612, 40, 1},
		{"1112", 1112, 0},
		{"1112", float64(1112), 0},
		{1112, "00:11:12", 0},
		{"11:12", 111200, 0},
		{"11:12", "11:12:00", 0},
		{"11:12:00", parseDuration("11h12m"), 0},
		{"-850:00:00", "-838:59:59", 0},
		{"850:00:00", "838:59:59", 0},
		{"-838:59:59.1", "-838:59:59", 0},
		{"838:59:59.1", "838:59:59", 0},
		{1112, "00:11:12.123", -1},
		{1112.123, "00:11:12", 1},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val1, test.val2), func(t *testing.T) {
			cmp, err := Time.Compare(test.val1, test.val2)
			require.NoError(t, err)
			assert.Equal(t, test.expectedCmp, cmp)
		})
	}
}

func TestTimeConvert(t *testing.T) {
	// This is here so that it doesn't pollute the namespace
	parseDuration := func(str string) time.Duration {
		d, err := time.ParseDuration(str)
		if err != nil {
			panic(err)
		}
		return d
	}

	tests := []struct {
		val interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{int8(-1), "-00:00:01", false},
		{uint8(59), "00:00:59", false},
		{"-1", "-00:00:01", false},
		{"59", "00:00:59", false},
		{parseDuration("62h22m48s379247us"), "62:22:48.379247", false},
		{int16(1002), "00:10:02", false},
		{uint16(5958), "00:59:58", false},
		{int32(15958), "01:59:58", false},
		{uint32(3332211), "333:22:11", false},
		{"1002", "00:10:02", false},
		{"5958", "00:59:58", false},
		{"15958", "01:59:58", false},
		{"3332211", "333:22:11", false},
		{float32(40.134613), "00:00:40.134613", false},
		{float64(401122.134612), "40:11:22.134612", false},
		{"40.134608", "00:00:40.134608", false},
		{"401122.134612", "40:11:22.134612", false},
		{"11:12", "11:12:00", false},
		{"-850:00:00", "-838:59:59", false},
		{"850:00:00", "838:59:59", false},
		{"-838:59:59.1", "-838:59:59", false},
		{"838:59:59.1", "838:59:59", false},

		{1060, nil, true},
		{60, nil, true},
		{6040, nil, true},
		{104060, nil, true},
		{106040, nil, true},
		{"1060", nil, true},
		{"60", nil, true},
		{"6040", nil, true},
		{"104060", nil, true},
		{"106040", nil, true},
		{"00:00:60", nil, true},
		{"00:60:00", nil, true},
		{-1060, nil, true},
		{-60, nil, true},
		{-6040, nil, true},
		{-104060, nil, true},
		{-106040, nil, true},
		{"-1060", nil, true},
		{"-60", nil, true},
		{"-6040", nil, true},
		{"-104060", nil, true},
		{"-106040", nil, true},
		{"-00:00:60", nil, true},
		{"-00:60:00", nil, true},
		{[]byte{0}, nil, true},
		{time.Date(2019, 12, 12, 12, 12, 12, 0, time.UTC), nil, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val, test.expectedVal), func(t *testing.T) {
			val, err := Time.Convert(test.val)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedVal, val)
			}
		})
	}
}

func TestTimeString(t *testing.T) {
	require.Equal(t, "TIME", Time.String())
}