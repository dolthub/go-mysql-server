package analyzer

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuickPerm(t *testing.T) {
	tests := []struct {
		name string
		inp  []int
		cmp  [][]int
		cnt  int
	}{
		{
			name: "3 tables",
			inp:  []int{0, 1, 2},
		},
		{
			name: "7 tables",
			inp:  []int{0, 1, 2, 3, 4, 5, 6},
		},
		{
			name: "12 tables",
			inp:  []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
		},
	}

	for _, tt := range tests {
		require := require.New(t)
		t.Run(tt.name, func(t *testing.T) {
			q := newQuickPerm(tt.inp)
			cnt := 0
			for {
				_, err := q.Next()
				if err == io.EOF {
					break
				}
				cnt += 1
			}
			exp := fact(len(tt.inp))
			require.Equal(exp, cnt)
		})
	}
}

func fact(n int) int {
	if n == 1 {
		return 1
	}
	return n * fact(n-1)
}
