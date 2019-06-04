// +build oniguruma

package regex

import (
	"time"

	rubex "github.com/src-d/go-oniguruma"
)

// Oniguruma holds a rubex regular expression Matcher.
type Oniguruma struct {
	reg *rubex.Regexp
}

// Match implements Matcher interface.
func (r *Oniguruma) Match(s string) bool {
	t := time.Now()
	defer MatchHistogram.With("string", s, "duration", "seconds").Observe(time.Since(t).Seconds())

	return r.reg.MatchString(s)
}

// Dispose implements Disposer interface.
// The function releases resources for oniguruma's precompiled regex
func (r *Oniguruma) Dispose() {
	r.reg.Free()
}

// NewOniguruma creates a new Matcher using oniguruma engine.
func NewOniguruma(re string) (Matcher, Disposer, error) {
	t := time.Now()
	reg, err := rubex.Compile(re)
	if err != nil {
		return nil, nil, err
	}
	CompileHistogram.With("regex", re, "duration", "seconds").Observe(time.Since(t).Seconds())

	r := Oniguruma{
		reg: reg,
	}
	return &r, &r, nil
}

func init() {
	err := Register("oniguruma", NewOniguruma)
	if err != nil {
		panic(err.Error())
	}
}
