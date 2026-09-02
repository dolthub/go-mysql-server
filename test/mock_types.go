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

package test

import "context"

// MockStringWrapper is a StringWrapper used for testing purposes
type MockStringWrapper struct {
	val string
}

func NewMockStringWrapper(val string) *MockStringWrapper {
	return &MockStringWrapper{val: val}
}

func (m MockStringWrapper) Unwrap(ctx context.Context) (string, error) {
	return m.val, nil
}

func (m MockStringWrapper) UnwrapAny(ctx context.Context) (interface{}, error) {
	return m.val, nil
}

func (m MockStringWrapper) IsExactLength() bool {
	return false
}

func (m MockStringWrapper) MaxByteLength() int64 {
	return int64(len(m.val))
}

func (m MockStringWrapper) Compare(ctx context.Context, other interface{}) (int, bool, error) {
	return 0, false, nil
}

func (m MockStringWrapper) Hash() interface{} {
	return m.val
}
