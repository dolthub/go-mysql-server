package planbuilder

import (
	"math"
	"strconv"
	"testing"

	mathrand "math/rand"
)

func makeIntStrs(n int) []string {
	res := make([]string, n)
	for i := 0; i < n; i++ {
		x := mathrand.Int63() - (math.MaxInt64 / 2)
		res[i] = strconv.FormatInt(x, 10)
	}
	return res
}

// BenchmarkConvertInt
// BenchmarkConvertInt-14		39405		29568 ns/op
// BenchmarkConvertIntNew-14	230546		5219 ns/op
func BenchmarkConvertInt(b *testing.B) {
	bld := &Builder{}
	vals := makeIntStrs(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, val := range vals {
			_ = bld.convertInt(val, 10)
		}
	}
}
