package regex

import "regexp"

// Go holds go regex engine Matcher.
type Go struct {
	reg *regexp.Regexp
}

// Match implements Matcher interface.
func (r *Go) Match(s string) bool {
	return r.reg.MatchString(s)
}

// NewGo creates a new Matcher using go regex engine.
func NewGo(re string) (Matcher, error) {
	reg, err := regexp.Compile(re)
	if err != nil {
		return nil, err
	}

	r := Go{
		reg: reg,
	}

	return &r, nil
}

func init() {
	err := Register("go", NewGo)
	if err != nil {
		panic(err.Error())
	}
}
