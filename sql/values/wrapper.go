// Copyright 2025 Dolthub, Inc.
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

package values

import "context"

// This file introduces interfaces for encapsulating values that may be expensive to load or deserialize.
// The engine will only unwrap these values if necessary for evaluating a query.
// A storage layer can return a value that implements one of these interfaces, and if the value is used in a write
// operation but not otherwise inspected, the engine may pass the wrapped value back to the storage layer.
// This is useful because the storage layer may not need to fully deserialize the value either.

// Example: If a table row contains a pointer to out-of-table storage, then a wrapper allows the pointer to be copied
// without ever needing to be dereferenced.

// JSONWrapper is an integrator specific implementation of a JSON field value.
// The query engine can utilize these optimized access methods improve performance
// by minimizing the need to unmarshall a JSONWrapper into a JSONDocument.
// TODO: Have JSONWrapper extend AnyWrapper
type JSONWrapper interface {
	// Clone creates a new value that can be mutated without affecting the original.
	Clone(ctx context.Context) JSONWrapper
	// ToInterface converts a JSONWrapper to an interface{} of simple types
	ToInterface() (interface{}, error)
}
