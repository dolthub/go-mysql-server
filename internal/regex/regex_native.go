package regex

import "regexp"

// Native holds go regex engine Matcher.
type Native struct {
	reg *regexp.Regexp
}

// Match implements Matcher interface.
func (r *Native) Match(s string) bool {
	return r.reg.MatchString(s)
}

// NewNative creates a new Matcher using go regex engine.
func NewNative(re string) (Matcher, error) {
	reg, err := regexp.Compile(re)
	if err != nil {
		return nil, err
	}

	r := Native{
		reg: reg,
	}

	return &r, nil
}

func init() {
	err := Register("native", NewNative)
	if err != nil {
		panic(err.Error())
	}
}
