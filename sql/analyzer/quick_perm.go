package analyzer

import "io"

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

func (p *quickPerm) Close() error {
	p.a = nil
	p.p = nil
	p.i = 0
	return nil
}
