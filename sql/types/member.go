// Copyright 2026 Dolthub, Inc.
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

package types

import "slices"

// memberPositions holds the new position of each member of an ENUM or SET,
// or -1 when the new type lacks it. Positions start at zero.
type memberPositions []int

// newMemberPositions returns the position in |to| of each name in |from|. A
// name missing from |to| gets -1.
func newMemberPositions(from, to []string) memberPositions {
	positions := make(memberPositions, len(from))
	for i, name := range from {
		positions[i] = slices.Index(to, name)
	}
	return positions
}

// IsIdentity implements the sql.MemberMap interface.
func (p memberPositions) IsIdentity() bool {
	for i, newPos := range p {
		if newPos != i {
			return false
		}
	}
	return true
}

// at returns the new position of the member at old position |i|. It returns
// false when |i| is out of range or the new type lacks that member.
func (p memberPositions) at(i int) (int, bool) {
	if i < 0 || i >= len(p) || p[i] < 0 {
		return 0, false
	}
	return p[i], true
}
