package dateparse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsers(t *testing.T) {
	tests := [...]struct {
		name             string
		chars            string
		parser           parser
		expectedRest     string
		expectedDatetime datetime
	}{
		{"24_timestamp", "13:12:15", parse24HourTimestamp, "",
			datetime{hours: uintPtr(13), minutes: uintPtr(12), seconds: uintPtr(15)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dt datetime
			rest, err := tt.parser(&dt, tt.chars)
			require.NoError(t, err)
			require.Equal(t, tt.expectedRest, rest)
			require.Equal(t, tt.expectedDatetime, dt)
		})
	}
}

func TestParserErr(t *testing.T) {
	tests := [...]struct {
		name        string
		chars       string
		parser      parser
		expectedErr string
	}{
		{"24_timestamp", "13:12", parse24HourTimestamp, `expected literal ":", found empty string`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dt datetime
			_, err := tt.parser(&dt, tt.chars)
			require.Error(t, err)
			require.Equal(t, tt.expectedErr, err.Error())
		})
	}
}

func uintPtr(u uint) *uint { return &u }
