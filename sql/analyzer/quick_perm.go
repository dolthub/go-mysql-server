package analyzer

import "io"

// quickPerm is an in-place permutation algorithm.
// The original array passed into quickPerm
// is mutated by every .Next() call.
// Use copy(newArray, perm) to save an intermediate
// result.
type quickPerm struct {
	a    []int
	n    int
	p    []int
	i    int
	init bool
}

func newQuickPerm(a []int) *quickPerm {
	return &quickPerm{
		a: a,
		n: len(a),
		p: make([]int, len(a)),
		i: 1,
	}
}

// Next returns the next array permutation
// refer to https://www.quickperm.org for pseudocode
func (p *quickPerm) Next() ([]int, error) {
	if !p.init {
		p.init = true
		return p.a, nil
	}
	for p.i < p.n {
		if p.p[p.i] < p.i {
			var j int
			if p.i%2 == 1 {
				j = p.p[p.i]
			} else {
				j = 0
			}
			p.a[p.i], p.a[j] = p.a[j], p.a[p.i]
			p.p[p.i]++
			p.i = 1
			return p.a, nil
		} else {
			p.p[p.i] = 0
			p.i++
		}
	}
	return nil, io.EOF
}
