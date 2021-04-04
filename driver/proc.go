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

package driver

import "sync"

// A ProcessManager manages IDs for connections and processes
type ProcessManager interface {
	// NextConnectionID returns the next unused connection ID
	NextConnectionID() uint32

	// NextProcessID returns the next unused process ID
	NextProcessID() uint64
}

// SimpleProcessManager returns incrementing IDs.
//
// The zero value of SimpleProcessManager is usable.
type SimpleProcessManager struct {
	mu     sync.Mutex
	connID uint32
	procID uint64
}

// NextConnectionID returns the next unused connection ID
func (m *SimpleProcessManager) NextConnectionID() uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connID++
	return m.connID
}

// NextProcessID returns the next unused process ID
func (m *SimpleProcessManager) NextProcessID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.procID++
	return m.procID
}
