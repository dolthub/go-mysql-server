// Copyright 2023 Dolthub, Inc.
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

package rowexec

import (
	"runtime/trace"

	"github.com/dolthub/go-mysql-server/sql"
)

var DefaultBuilder = &BaseBuilder{}

var _ sql.NodeExecBuilder = (*BaseBuilder)(nil)

type ExecBuilderFunc func(ctx *sql.Context, n sql.Node, r sql.Row) (sql.RowIter, error)

// BaseBuilder converts a plan tree into a RowIter tree. All relational nodes
// have a build statement. Custom source nodes that provide rows that implement
// sql.ExecSourceRel are also built into the tree.
type BaseBuilder struct {
	// if override is provided, we try to build executor with this first
	override                sql.NodeExecBuilder
	triggerSavePointCounter int // tracks the number of save points that have been created by triggers
}

func (b *BaseBuilder) Build(ctx *sql.Context, n sql.Node, r sql.Row) (sql.RowIter, error) {
	defer trace.StartRegion(ctx, "ExecBuilder.Build").End()
	return b.buildNodeExec(ctx, n, r)
}

func NewOverrideBuilder(override sql.NodeExecBuilder) sql.NodeExecBuilder {
	return &BaseBuilder{override: override}
}
