// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
		val1        interface{}
		val2        interface{}
		expectedCmp int
	}{
		{nil, 0, 1},
		{0, nil, -1},
		{nil, nil, 0},
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
		{"11:12:00.1234567", "11:12:00.1234569", 0},
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
		val         interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{nil, nil, false},
		{int8(-1), "-00:00:01", false},
		{uint8(59), "00:00:59", false},
		{"-1", "-00:00:01", false},
		{"59", "00:00:59", false},
		{parseDuration("62h22m48s379247us"), "62:22:48.379247", false},
		{int16(1002), "00:10:02", false},
		{uint16(5958), "00:59:58", false},
		{int32(15958), "01:59:58", false},
		{uint32(3332211), "333:22:11", false},
		{int16(-1002), "-00:10:02", false},
		{int16(-5958), "-00:59:58", false},
		{int32(-15958), "-01:59:58", false},
		{int32(-3332211), "-333:22:11", false},
		{"1002", "00:10:02", false},
		{"902", "00:09:02", false},
		{902, "00:09:02", false},
		{float64(-0.25), "-00:00:00.250000", false},
		{float64(-46.25), "-00:00:46.250000", false},
		{float64(-56.75), "-00:00:56.750000", false},
		{float64(-256.75), "-00:02:56.750000", false},
		{float64(-122256.5), "-12:22:56.500000", false},
		{float64(902), "00:09:02", false},
		{"00:05:55.2", "00:05:55.200000", false},
		{"555.2", "00:05:55.200000", false},
		{555.2, "00:05:55.200000", false},
		{"5958", "00:59:58", false},
		{"15958", "01:59:58", false},
		{"3332211", "333:22:11", false},
		{float32(40.134613), "00:00:40.134613", false},
		{float64(401122.134612), "40:11:22.134612", false},
		{"40.134608", "00:00:40.134608", false},
		{"401122.134612", "40:11:22.134612", false},
		{"401122.134612585", "40:11:22.134613", false},
		{"595959.99999951", "59:59:59.999999", false},
		{"585859.999999514", "58:58:59.999999", false},
		{"40:11:22.134612585", "40:11:22.134613", false},
		{"59:59:59.9999995", "60:00:00", false},
		{"58:59:59.99999951", "58:59:59.999999", false},
		{"58:58:59.999999514", "58:58:59.999999", false},
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
				if test.val == nil {
					assert.Equal(t, test.expectedVal, val)
				} else {
					assert.Equal(t, test.expectedVal, val.(Timespan).String())
					timespan, err := Time.ConvertToTimespan(test.val)
					require.NoError(t, err)
					require.True(t, timespan.Equals(val.(Timespan)))
					ms := timespan.AsMicroseconds()
					ums := Time.MicrosecondsToTimespan(ms)
					cmp, err := Time.Compare(test.val, ums)
					require.NoError(t, err)
					assert.Equal(t, 0, cmp)
				}
			}
		})
	}
}

func TestTimeConvertToTimeDuration(t *testing.T) {
	// This is here so that it doesn't pollute the namespace
	parseDuration := func(str string) time.Duration {
		d, err := time.ParseDuration(str)
		if err != nil {
			panic(err)
		}
		return d
	}

	tests := []struct {
		val         string
		expectedVal time.Duration
	}{
		{"-00:00:01", parseDuration("-1s")},
		{"00:00:59", parseDuration("59s")},
		{"62:22:48.379247", parseDuration("62h22m48s379247µs")},
		{"00:10:02", parseDuration("10m2s")},
		{"00:59:58", parseDuration("59m58s")},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val, test.expectedVal), func(t *testing.T) {
			val, err := Time.ConvertToTimeDuration(test.val)
			require.NoError(t, err)
			assert.Equal(t, test.expectedVal, val)
		})
	}
}

func TestTimeString(t *testing.T) {
	require.Equal(t, "time(6)", Time.String())
}

func TestTimeZero(t *testing.T) {
	_, ok := Time.Zero().(Timespan)
	require.True(t, ok)
}
