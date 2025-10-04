package regex_test

import (
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/internal/regex"
)

func TestMatches(t *testing.T) {
	tests := map[int]struct {
		reg   string
		flag  regex.RegexFlags
		str   string
		start int
		occ   int
		exp   bool
	}{
		0: {
			reg: `abc+.*this st`,
			str: "Find the abc in this string",
			exp: true,
		},
		1: {
			reg: `abc+.*this st`,
			str: "Find the abc in this here string",
			exp: false,
		},
		2: {
			reg: `[a-zA-Z0-9]{5} \w{4} aab`,
			str: "Words like aab don't exist",
			exp: true,
		},
		3: {
			reg: `^[aA]bcd[eE]$`,
			str: "abcde",
			exp: true,
		},
		4: {
			reg: `^[aA]bcd[eE]$`,
			str: "Abcde",
			exp: true,
		},
		5: {
			reg: `^[aA]bcd[eE]$`,
			str: "AbcdE",
			exp: true,
		},
	}

	for _, test := range tests {
		name := fmt.Sprintf(`%q/%q`, test.reg, test.str)

		t.Run(name, func(t *testing.T) {
			re := regex.CreateRegex(1024)
			defer re.Close()
			if err := re.SetRegexString(t.Context(), test.reg, test.flag); err != nil {
				t.Error(err)
			}
			if err := re.SetMatchString(t.Context(), test.str); err != nil {
				t.Error(err)
			}

			r, err := re.Matches(t.Context(), test.start, test.occ)
			if err != nil {
				t.Error(err)
			}
			if r != test.exp {
				t.Errorf("Matches = %v, wants %v", r, test.exp)
			}
		})
	}
}

func TestReplace(t *testing.T) {
	re := regex.CreateRegex(1024)
	defer re.Close()
	if err := re.SetRegexString(t.Context(), `[a-z]+`, regex.RegexFlags_None); err != nil {
		t.Fatal(err)
	}
	if err := re.SetMatchString(t.Context(), "abc def ghi"); err != nil {
		t.Fatal(err)
	}

	tests := map[int]struct {
		pos int
		occ int
		exp string
	}{
		0: {
			pos: 1,
			occ: 2,
			exp: "abc X ghi",
		},
		1: {
			pos: 1,
			occ: 3,
			exp: "abc def X",
		},
		2: {
			pos: 1,
			occ: 0,
			exp: "X X X",
		},
		4: {
			pos: 1,
			occ: 4,
			exp: "abc def ghi",
		},
	}
	for _, test := range tests {
		name := fmt.Sprintf("[%d,%d]", test.pos, test.occ)

		t.Run(name, func(t *testing.T) {
			r, err := re.Replace(t.Context(), "X", test.pos, test.occ)
			if err != nil {
				t.Error(err)
			}
			if r != test.exp {
				t.Errorf("Replace = %q, wants %q", r, test.exp)
			}
		})
	}
}

func TestIndexOf(t *testing.T) {
	re := regex.CreateRegex(1024)
	defer re.Close()
	if err := re.SetRegexString(t.Context(), `[a-j]+`, regex.RegexFlags_None); err != nil {
		t.Fatal(err)
	}

	tests := map[int]struct {
		str   string
		start int
		occ   int
		endi  bool
		exp   int
	}{
		0: {
			str:   "abc def ghi",
			start: 1,
			occ:   1,
			endi:  false,
			exp:   1,
		},
		1: {
			str:   "abc def ghi",
			start: 4,
			occ:   1,
			endi:  false,
			exp:   5,
		},
		2: {
			str:   "abc def ghi",
			start: 8,
			occ:   1,
			endi:  false,
			exp:   9,
		},
		3: {
			str:   "abc def ghi",
			start: 1,
			occ:   3,
			endi:  false,
			exp:   9,
		},
		4: {
			str:   "abc def ghi",
			start: 1,
			occ:   4,
			endi:  false,
			exp:   0,
		},
		5: {
			str:   "abc def ghi",
			start: 1,
			occ:   1,
			endi:  true,
			exp:   4,
		},
		6: {
			str:   "abc def ghi",
			start: 4,
			occ:   1,
			endi:  true,
			exp:   8,
		},
		7: {
			str:   "abc def ghi",
			start: 8,
			occ:   1,
			endi:  true,
			exp:   12,
		},
		8: {
			str:   "abc def ghi",
			start: 1,
			occ:   2,
			endi:  true,
			exp:   8,
		},
		9: {
			str:   "abc def ghi",
			start: 1,
			occ:   3,
			endi:  true,
			exp:   12,
		},
		10: {
			str:   "abc def ghi",
			start: 1,
			occ:   4,
			endi:  true,
			exp:   0,
		},
		11: {
			str:   "klmno fghij abcde",
			start: 1,
			occ:   1,
			endi:  false,
			exp:   7,
		},
		12: {
			str:   "klmno fghij abcde",
			start: 1,
			occ:   1,
			endi:  true,
			exp:   12,
		},
	}

	for _, test := range tests {
		name := fmt.Sprintf("%q/[%d,%d,%v]", test.str, test.start, test.occ, test.endi)
		t.Run(name, func(t *testing.T) {
			if err := re.SetMatchString(t.Context(), test.str); err != nil {
				t.Fatal(err)
			}
			r, err := re.IndexOf(t.Context(), test.start, test.occ, test.endi)
			if err != nil {
				t.Error(err)
			}
			if r != test.exp {
				t.Errorf("IndexOf = %v, wants %v", r, test.exp)
			}
		})
	}
}

func TestSubstring(t *testing.T) {
	re := regex.CreateRegex(1024)
	defer re.Close()
	if err := re.SetRegexString(t.Context(), `[a-z]+`, regex.RegexFlags_None); err != nil {
		t.Fatal(err)
	}

	tests := map[int]struct {
		str   string
		start int
		occ   int
		expb  bool
		exps  string
	}{
		0: {
			str:   "abc def ghi",
			start: 1,
			occ:   1,
			expb:  true,
			exps:  "abc",
		},
		1: {
			str:   "abc def ghi",
			start: 4,
			occ:   1,
			expb:  true,
			exps:  "def",
		},
		2: {
			str:   "abc def ghi",
			start: 8,
			occ:   1,
			expb:  true,
			exps:  "ghi",
		},
		3: {
			str:   "abc def ghi",
			start: 1,
			occ:   2,
			expb:  true,
			exps:  "def",
		},
		4: {
			str:   "abc def ghi",
			start: 1,
			occ:   3,
			expb:  true,
			exps:  "ghi",
		},
		5: {
			str:   "abc def ghi",
			start: 1,
			occ:   4,
			expb:  false,
			exps:  "",
		},
		6: {
			str:   "ghx dey abz",
			start: 1,
			occ:   1,
			expb:  true,
			exps:  "ghx",
		},
	}

	for _, test := range tests {
		name := fmt.Sprintf("%q/[%d,%d]", test.str, test.start, test.occ)
		t.Run(name, func(t *testing.T) {
			if err := re.SetMatchString(t.Context(), test.str); err != nil {
				t.Fatal(err)
			}
			rs, rb, err := re.Substring(t.Context(), test.start, test.occ)
			if err != nil {
				t.Error(err)
			}
			if rs != test.exps || rb != test.expb {
				t.Errorf("IndexOf = (%q, %v), wants (%q, %v)", rs, rb, test.exps, test.expb)
			}
		})
	}
}

func TestCaseSensitivity(t *testing.T) {
	tests := map[int]struct {
		reg  string
		flag regex.RegexFlags
		str  string
		exp  bool
	}{
		0: {
			reg:  `abc`,
			flag: regex.RegexFlags_Case_Insensitive,
			str:  "ABC",
			exp:  true,
		},
		1: {
			reg:  `abc`,
			flag: regex.RegexFlags_None,
			str:  "ABC",
			exp:  false,
		},
	}

	for _, test := range tests {
		name := fmt.Sprintf("%q(%v)/%q", test.reg, test.flag, test.str)
		t.Run(name, func(t *testing.T) {
			re := regex.CreateRegex(1024)
			defer re.Close()
			if err := re.SetRegexString(t.Context(), test.reg, test.flag); err != nil {
				t.Fatal(err)
			}
			if err := re.SetMatchString(t.Context(), test.str); err != nil {
				t.Fatal(err)
			}
			r, err := re.Matches(t.Context(), 0, 0)
			if err != nil {
				t.Fatalf("Matches error: %v", err)
			}
			if r != test.exp {
				t.Fatalf("Matches = %v, wants %v", r, test.exp)
			}
		})
	}
}

func TestReplace2(t *testing.T) {
	re := regex.CreateRegex(1024)
	defer re.Close()
	if err := re.SetRegexString(t.Context(), `[0-4]`, regex.RegexFlags_None); err != nil {
		t.Fatal(err)
	}
	if err := re.SetMatchString(t.Context(), "0123456789"); err != nil {
		t.Fatal(err)
	}

	tests := map[int]struct {
		pos int
		occ int
		exp string
	}{
		0: {
			pos: 1,
			occ: 0,
			exp: "XXXXX56789",
		},
		1: {
			pos: 2,
			occ: 0,
			exp: "0XXXX56789",
		},
		2: {
			pos: 3,
			occ: 2,
			exp: "012X456789",
		},
	}

	for _, test := range tests {
		name := fmt.Sprintf("[%d,%d]", test.pos, test.occ)
		t.Run(name, func(t *testing.T) {
			r, err := re.Replace(t.Context(), "X", test.pos, test.occ)
			if err != nil {
				t.Error(err)
			}
			if r != test.exp {
				t.Errorf("Replace = %q, wants %q", r, test.exp)
			}
		})
	}
}
