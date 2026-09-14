// Copyright 2020-2026 Dolthub, Inc.
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

package testutils

// CustomValueValidator is an interface for custom validation of values in the result set
type CustomValueValidator interface {
	Validate(interface{}) (bool, error)
}

// isUUIDString is a CustomValueValidator for UUID strings
type IsUUIDString struct{}

func (IsUUIDString) Validate(v interface{}) (bool, error) {
	s, ok := v.(string)
	return ok && len(s) == 36, nil
}
