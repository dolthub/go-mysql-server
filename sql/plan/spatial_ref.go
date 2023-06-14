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

package plan

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type SrsAttribute struct {
	Name         string
	Definition   string
	Organization string
	OrgID        uint32
	Description  string
}

// CreateSpatialRefSys represents the statement CREATE SPATIAL REFERENCE SYSTEM ...
type CreateSpatialRefSys struct {
	SRID        uint32
	OrReplace   bool
	IfNotExists bool
	SrsAttr     SrsAttribute
}

var _ sql.Node = (*CreateSpatialRefSys)(nil)

func NewCreateSpatialRefSys(srid uint32, orReplace, ifNotExists bool, srsAttr SrsAttribute) (sql.Node, error) {
	return &CreateSpatialRefSys{
		SRID:        srid,
		OrReplace:   orReplace,
		IfNotExists: ifNotExists,
		SrsAttr:     srsAttr,
	}, nil
}

// Resolved implements the interface sql.Node
func (n *CreateSpatialRefSys) Resolved() bool {
	return true
}

// String implements the interface sql.Node
func (n *CreateSpatialRefSys) String() string {
	return ""
}

// Schema implements the interface sql.Node
func (n *CreateSpatialRefSys) Schema() sql.Schema {
	return types.OkResultSchema
}

// Children implements the interface sql.Node
func (n *CreateSpatialRefSys) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node
func (n *CreateSpatialRefSys) WithChildren(children ...sql.Node) (sql.Node, error) {
	return nil, nil
}

// CheckPrivileges implements the interface sql.Node
func (n *CreateSpatialRefSys) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation("mysql", "st_spatial_references_systems", "", sql.PrivilegeType_Insert))
}
