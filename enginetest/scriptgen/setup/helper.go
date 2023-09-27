// Copyright 2022 Dolthub, Inc.
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

package setup

var (
	KeylessSetup         = [][]SetupScript{MydbData, KeylessData}
	VersionedSetup       = [][]SetupScript{MydbData, MyhistorytableData}
	OrdinalSetup         = [][]SetupScript{MydbData, Invert_pkData, Ordinals_ddlData}
	FooSetup             = [][]SetupScript{MydbData, FooData}
	JsonSetup            = [][]SetupScript{MydbData, JsontableData}
	SpatialSetup         = [][]SetupScript{MydbData, SpatialData}
	PksSetup             = [][]SetupScript{MydbData, Pk_tablesData}
	GraphSetup           = [][]SetupScript{MydbData, Graph_tablesData}
	ReservedSetup        = [][]SetupScript{MydbData, Reserved_keywordsData}
	Mytable              = [][]SetupScript{MydbData, MytableData}
	ChecksSetup          = [][]SetupScript{MydbData, Check_constraintData}
	NullsSetup           = [][]SetupScript{MydbData, Null_rangesData}
	ComplexIndexSetup    = [][]SetupScript{MydbData, Comp_index_tablesData}
	ImdbPlanSetup        = [][]SetupScript{MydbData, ImdbData}
	TpchPlanSetup        = [][]SetupScript{MydbData, TpchData}
	TpccPlanSetup        = [][]SetupScript{MydbData, TpccData}
	TpcdsPlanSetup       = [][]SetupScript{MydbData, TpcdsData}
	LoadDataSetup        = [][]SetupScript{MydbData, LoadtableData}
	XySetup              = [][]SetupScript{MydbData, XyData}
	JoinsSetup           = [][]SetupScript{MydbData, JoinData}
	IntegrationPlanSetup = [][]SetupScript{MydbData, Integration_testData}

	SpecialSetup = [][]SetupScript{
		MydbData,
		AutoincrementData,
		BigtableData,
		DatetimetableData,
		EmptytableData,
		Fk_tblData,
		FloattableData,
		NewlinetableData,
		NiltableData,
		OthertableData,
		SpecialtableData,
		StringandtableData,
		TabletestData,
		TypestableData,
		PeopleData,
		Reserved_keywordsData,
	}

	SimpleSetup = [][]SetupScript{
		MydbData,
		MytableData,
		AutoincrementData,
		BigtableData,
		DatetimetableData,
		EmptytableData,
		Fk_tblData,
		FloattableData,
		NewlinetableData,
		NiltableData,
		OthertableData,
		Invert_pkData,
		SpecialtableData,
		StringandtableData,
		TabletestData,
		TypestableData,
		PeopleData,
		Reserved_keywordsData,
		Pk_tablesData,
		Ordinals_ddlData,
		JsontableData,
		MyhistorytableData,
		KeylessData,
		FooData,
		Graph_tablesData,
		XyData,
	}

	PlanSetup = [][]SetupScript{
		MydbData,
		MytableData,
		AutoincrementData,
		BigtableData,
		DatetimetableData,
		EmptytableData,
		Fk_tblData,
		FloattableData,
		NewlinetableData,
		NiltableData,
		OthertableData,
		SpecialtableData,
		StringandtableData,
		TabletestData,
		TypestableData,
		PeopleData,
		Reserved_keywordsData,
		Pk_tablesData,
		Ordinals_ddlData,
		JsontableData,
		MyhistorytableData,
		KeylessData,
		FooData,
		Graph_tablesData,
		XyData,
		Invert_pkData,
		JoinData,
	}
)
