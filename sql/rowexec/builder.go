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
	"github.com/dolthub/go-mysql-server/sql"
)

var DefaultBuilder = &BaseBuilder{}

var _ sql.NodeExecBuilder = (*BaseBuilder)(nil)

type ExecBuilderFunc func(ctx *sql.Context, n sql.Node, r sql.Row) (sql.RowIter, error)

type BaseBuilder struct {
	customSources ExecBuilderFunc
}

func (b *BaseBuilder) WithCustomSources(custom ExecBuilderFunc) *BaseBuilder {
	b.customSources = custom
	return b
}

func (b *BaseBuilder) Build(ctx *sql.Context, n sql.Node, r sql.Row) (sql.RowIter, error) {
	if b.customSources != nil {
		if ret, err := b.customSources(ctx, n, r); err != nil {
			return nil, err
		} else if ret != nil {
			return ret, nil
		}
	}
	return b.buildNodeExec(ctx, n, r)
}
