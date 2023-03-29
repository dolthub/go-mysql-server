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
	Mytable           [][]SetupScript
	KeylessSetup      [][]SetupScript
	VersionedSetup    [][]SetupScript
	SpecialSetup      [][]SetupScript
	SimpleSetup       [][]SetupScript
	PlanSetup         [][]SetupScript
	OrdinalSetup      [][]SetupScript
	SpatialSetup      [][]SetupScript
	JsonSetup         [][]SetupScript
	FooSetup          [][]SetupScript
	GraphSetup        [][]SetupScript
	ReservedSetup     [][]SetupScript
	ChecksSetup       [][]SetupScript
	NullsSetup        [][]SetupScript
	ComplexIndexSetup [][]SetupScript
	LoadDataSetup     [][]SetupScript
	XySetup           [][]SetupScript
)

func init() {
	KeylessSetup = [][]SetupScript{MydbData, KeylessData}
	VersionedSetup = [][]SetupScript{MydbData, MyhistorytableData}
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
	OrdinalSetup = [][]SetupScript{MydbData, Invert_pkData, Ordinals_ddlData}
	FooSetup = [][]SetupScript{MydbData, FooData}
	JsonSetup = [][]SetupScript{MydbData, JsontableData}
	SpatialSetup = [][]SetupScript{MydbData, SpatialData}
	PksSetup := [][]SetupScript{MydbData, Pk_tablesData}
	GraphSetup = [][]SetupScript{MydbData, Graph_tablesData}
	ReservedSetup = [][]SetupScript{MydbData, Reserved_keywordsData}
	Mytable = [][]SetupScript{MydbData, MytableData}
	ChecksSetup = [][]SetupScript{MydbData, Check_constraintData}
	NullsSetup = [][]SetupScript{MydbData, Null_rangesData}
	ComplexIndexSetup = [][]SetupScript{MydbData, Comp_index_tablesData}
	LoadDataSetup = [][]SetupScript{MydbData, LoadtableData}
	XySetup = [][]SetupScript{MydbData, XyData}
	SimpleSetup = concatenateSetupSources(
		Mytable,
		SpecialSetup,
		PksSetup,
		OrdinalSetup,
		JsonSetup,
		VersionedSetup,
		KeylessSetup,
		FooSetup,
		GraphSetup,
		XySetup,
	)
	PlanSetup = concatenateSetupSources(
		Mytable,
		SpecialSetup,
		PksSetup,
		OrdinalSetup,
		JsonSetup,
		VersionedSetup,
		KeylessSetup,
		FooSetup,
		GraphSetup,
		XySetup,
	)
}

func concatenateSetupSources(in ...[][]SetupScript) [][]SetupScript {
	out := make([][]SetupScript, 0)
	for i := range in {
		out = append(out, in[i]...)
	}
	return out
}
