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
