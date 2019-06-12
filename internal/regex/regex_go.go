package regex

import (
	"regexp"
	"time"
)

// Go holds go regex engine Matcher.
type Go struct {
	reg *regexp.Regexp
}

// Match implements Matcher interface.
func (r *Go) Match(s string) bool {
	t := time.Now()
	defer MatchHistogram.With("string", s, "duration", "seconds").Observe(time.Since(t).Seconds())

	return r.reg.MatchString(s)
}

// Dispose implements Disposer interface.
func (*Go) Dispose() {}

// NewGo creates a new Matcher using go regex engine.
func NewGo(re string) (Matcher, Disposer, error) {
	t := time.Now()
	reg, err := regexp.Compile(re)
	if err != nil {
		return nil, nil, err
	}
	CompileHistogram.With("regex", re, "duration", "seconds").Observe(time.Since(t).Seconds())

	r := Go{
		reg: reg,
	}
	return &r, &r, nil
}

func init() {
	err := Register("go", NewGo)
	if err != nil {
		panic(err.Error())
	}
}
