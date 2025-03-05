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

package sql

import (
	"context"
)

// This file introduces interfaces for encapsulating values that may be expensive to load or deserialize.
// The engine will only unwrap these values if necessary for evaluating a query.
// A storage layer can return a value that implements one of these interfaces, and if the value is used in a write
// operation but not otherwise inspected, the engine may pass the wrapped value back to the storage layer.
// This is useful because the storage layer may not need to fully deserialize the value either.

// Example: If a table row contains a pointer to out-of-table storage, then a wrapper allows the pointer to be copied
// without ever needing to be dereferenced.

// AnyWrapper is an interface for types that encapsulate some SQL value.
type AnyWrapper interface {
	// UnwrapAny "unwraps" an AnyWrapper into a simple type.
	UnwrapAny(ctx context.Context) (interface{}, error)
	// MaxLength is the maximum possible length of the value in bytes. Converting the value to type with a shorter max length will
	// trigger the value to be unwrapped.
	//MaxLength() int64
	// Currently only wrapped strings/blobs are supported. This could be changed to sql.Type in the future.
	StringType() StringType
}

// Wrapper is an interface for types that encapsulate a SQL value of a specific type.
// Implementations are also required to implement AnyWrapper.
type Wrapper[T any] interface {
	AnyWrapper
	Unwrap(ctx context.Context) (result T, err error)
}

type StringWrapper = Wrapper[string]
type BytesWrapper = Wrapper[[]byte]

// UnwrapAny takes a possibly-wrapped value and unwraps it. If the input isn't a wrapper, the input is returned unmodified.
func UnwrapAny(ctx context.Context, v interface{}) (result interface{}, err error) {
	switch vv := v.(type) {
	case AnyWrapper:
		return vv.UnwrapAny(ctx)
	case JSONWrapper:
		return vv.ToInterface()
	}
	return v, nil
}

// Unwrap takes a possibly-wrapped value and attempts to unwrap it into the requested type.
// If the input isn't a wrapper for the specified type, |ok| is set to false.
func Unwrap[T any](ctx context.Context, v interface{}) (result T, ok bool, err error) {
	switch vv := v.(type) {
	case Wrapper[T]:
		result, err = vv.Unwrap(ctx)
		return result, true, err
	case T:
		return vv, true, nil
	}
	return result, false, nil
}

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
