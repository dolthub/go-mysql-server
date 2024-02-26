// Copyright 2024 Dolthub, Inc.
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

package plan_errors

import "gopkg.in/src-d/go-errors.v1"

var (
	// ErrNotIndexable is returned when the table is not indexable.
	ErrNotIndexable = errors.NewKind("the table is not indexable")

	// ErrInvalidIndexDriver is returned when the index driver can't be found.
	ErrInvalidIndexDriver = errors.NewKind("invalid driver index %q")

	// ErrExprTypeNotIndexable is returned when the expression type cannot be
	// indexed, such as BLOB or JSON.
	ErrExprTypeNotIndexable = errors.NewKind("expression %q with type %s cannot be indexed")
)
