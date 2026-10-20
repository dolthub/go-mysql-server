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

// Package setup re-exports the setup scripts from
// github.com/dolthub/go-mysql-server/enginetest/setup for backwards
// compatibility.
//
// Deprecated: use github.com/dolthub/go-mysql-server/enginetest/setup.
package setup

import (
	"github.com/dolthub/go-mysql-server/enginetest/setup"
)

type SetupScript = setup.SetupScript

var (
	KeylessSetup                = setup.KeylessSetup
	VersionedSetup              = setup.VersionedSetup
	OrdinalSetup                = setup.OrdinalSetup
	FooSetup                    = setup.FooSetup
	JsonSetup                   = setup.JsonSetup
	SpatialSetup                = setup.SpatialSetup
	PksSetup                    = setup.PksSetup
	GraphSetup                  = setup.GraphSetup
	ReservedSetup               = setup.ReservedSetup
	GeneratedColumnSetup        = setup.GeneratedColumnSetup
	SysbenchSetup               = setup.SysbenchSetup
	Mytable                     = setup.Mytable
	ChecksSetup                 = setup.ChecksSetup
	NullsSetup                  = setup.NullsSetup
	ComplexIndexSetup           = setup.ComplexIndexSetup
	ImdbPlanSetup               = setup.ImdbPlanSetup
	TpchPlanSetup               = setup.TpchPlanSetup
	TpccPlanSetup               = setup.TpccPlanSetup
	TpcdsPlanSetup              = setup.TpcdsPlanSetup
	LoadDataSetup               = setup.LoadDataSetup
	XySetup                     = setup.XySetup
	JoinsSetup                  = setup.JoinsSetup
	IntegrationPlanSetup        = setup.IntegrationPlanSetup
	SpecialSetup                = setup.SpecialSetup
	SimpleSetup                 = setup.SimpleSetup
	PlanSetup                   = setup.PlanSetup
	AutoincrementData           = setup.AutoincrementData
	BigtableData                = setup.BigtableData
	BlobData                    = setup.BlobData
	Check_constraintData        = setup.Check_constraintData
	Comp_index_tablesData       = setup.Comp_index_tablesData
	DatetimetableData           = setup.DatetimetableData
	EmptytableData              = setup.EmptytableData
	ExplodeData                 = setup.ExplodeData
	Fk_tblData                  = setup.Fk_tblData
	FloattableData              = setup.FloattableData
	FooData                     = setup.FooData
	Generated_column_tablesData = setup.Generated_column_tablesData
	Graph_tablesData            = setup.Graph_tablesData
	Id_parentData               = setup.Id_parentData
	ImdbData                    = setup.ImdbData
	Integration_testData        = setup.Integration_testData
	Invert_pkData               = setup.Invert_pkData
	JoinData                    = setup.JoinData
	JsontableData               = setup.JsontableData
	KeylessData                 = setup.KeylessData
	Keyless_idxData             = setup.Keyless_idxData
	LoadtableData               = setup.LoadtableData
	MydbData                    = setup.MydbData
	MyhistorytableData          = setup.MyhistorytableData
	MytableData                 = setup.MytableData
	Mytable_del_idxData         = setup.Mytable_del_idxData
	NewlinetableData            = setup.NewlinetableData
	NiltableData                = setup.NiltableData
	Null_rangesData             = setup.Null_rangesData
	Ordinals_ddlData            = setup.Ordinals_ddlData
	OthertableData              = setup.OthertableData
	Othertable_del_idxData      = setup.Othertable_del_idxData
	Parent_childData            = setup.Parent_childData
	PeopleData                  = setup.PeopleData
	Pk_tablesData               = setup.Pk_tablesData
	Reserved_keywordsData       = setup.Reserved_keywordsData
	SpatialData                 = setup.SpatialData
	SpecialtableData            = setup.SpecialtableData
	StringandtableData          = setup.StringandtableData
	SysbenchData                = setup.SysbenchData
	TabletestData               = setup.TabletestData
	TestdbData                  = setup.TestdbData
	TpccData                    = setup.TpccData
	TpcdsData                   = setup.TpcdsData
	TpchData                    = setup.TpchData
	TypestableData              = setup.TypestableData
	ViewsData                   = setup.ViewsData
	XyData                      = setup.XyData
)
