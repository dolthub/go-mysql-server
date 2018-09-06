// +build oniguruma

package regex

import "github.com/moovweb/rubex"

// Oniguruma holds a rubex regular expression Matcher.
type Oniguruma struct {
	reg *rubex.Regexp
}

// Match implements Matcher interface.
func (r *Oniguruma) Match(s string) bool {
	return r.reg.MatchString(s)
}

// NewOniguruma creates a new Matcher using oniguruma engine.
func NewOniguruma(re string) (Matcher, error) {
	reg, err := rubex.Compile(re)
	if err != nil {
		return nil, err
	}

	r := Oniguruma{
		reg: reg,
	}

	return &r, nil
}

func init() {
	err := Register("oniguruma", NewOniguruma)
	if err != nil {
		panic(err.Error())
	}
}
