package test

import "testing"

var X [1 << 15]struct {
	val int
	_   [4096]byte
}

var Result int

func BenchmarkRangeTwoParam(b *testing.B) {
	var r int
	for n := 0; n < b.N; n++ {
		for _, x := range X {
			r += x.val
		}
	}
	Result = r
}

func BenchmarkRangeOneParam(b *testing.B) {
	var r int
	for n := 0; n < b.N; n++ {
		for i := range X {
			x := &X[i]
			r += x.val
		}
	}
	Result = r
}

func BenchmarkFor(b *testing.B) {
	var r int
	for n := 0; n < b.N; n++ {
		for i := 0; i < len(X); i++ {
			x := &X[i]
			r += x.val
		}
	}
	Result = r
}
