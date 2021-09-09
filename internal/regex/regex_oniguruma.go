// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build oniguruma
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
