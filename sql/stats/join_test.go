package stats

import (
	"github.com/dolthub/go-mysql-server/sql"
	"testing"
)

// TODO generate distributions

func TestJoin(t *testing.T) {
	tests := []struct {
		left  []sql.HistogramBucket
		right []sql.HistogramBucket
		equiv sql.EquivSets
	}{
		{},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {

		})
	}
}
