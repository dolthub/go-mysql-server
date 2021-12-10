// Copyright 2021 Dolthub, Inc.
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

package plan

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

type KillType int
const (
	KillType_Query KillType = 0
	KillType_Connection KillType = 1
)

var _ sql.Node = (*Kill)(nil)

func (kt KillType) String() string {
	if kt == KillType_Query {
		return "QUERY"
	} else if kt == KillType_Connection {
		return "CONNECTION"
	}
	panic(fmt.Sprintf("Invalid KillType value %d", kt))
}

type Kill struct {
	kt KillType
	connID uint32
}

func NewKill(kt KillType, connID uint32) *Kill {
	return &Kill{kt, connID}
}

func (k *Kill) Resolved() bool {
	return true
}

func (k *Kill) Children() []sql.Node {
	return nil
}

func (k *Kill) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(k, len(children), 0)
	}
	return k, nil
}

func (k *Kill) Schema() sql.Schema {
	return sql.OkResultSchema
}

func (k *Kill) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// TODO: Maybe don't do this kill until Next() is called.
	ctx.ProcessList.Kill(k.connID)
	if k.kt == KillType_Connection {
		ctx.KillConnection(k.connID)
	}
	return sql.RowsToRowIter(sql.NewRow(sql.NewOkResult(0))), nil
}

func (k *Kill) String() string {
	return fmt.Sprintf("KILL %s %d", k.kt.String(), k.connID)
}
