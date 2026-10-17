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

package setup

import (
	"embed"
	"errors"
	"fmt"
	"io"
	"path"
)

//go:embed data
var dataFS embed.FS

const dataDir = "data"

// loadedScripts records every script loaded by mustLoad, so tests can check
// that each embedded script file has a corresponding exported variable.
var loadedScripts = make(map[string]struct{})

// mustLoad parses the embedded setup script |name| into a SetupScript. It
// panics if the script does not exist or fails to parse.
func mustLoad(name string) []SetupScript {
	script, err := load(name)
	if err != nil {
		panic(fmt.Sprintf("enginetest setup: loading script %q: %v", name, err))
	}
	loadedScripts[name] = struct{}{}
	return []SetupScript{script}
}

func load(name string) (SetupScript, error) {
	p := path.Join(dataDir, name)
	f, err := dataFS.Open(p)
	if err != nil {
		return nil, err
	}
	s := newFileSetup(p, f)
	defer s.Close()
	var script SetupScript
	for {
		_, err := s.Next()
		if errors.Is(err, io.EOF) {
			return script, nil
		} else if err != nil {
			return nil, err
		}
		script = append(script, s.Data().Sql)
	}
}

// Each file in data/ must have a variable here; TestEveryScriptIsLoaded
// enforces this.
var (
	AutoincrementData           = mustLoad("autoincrement")
	BigtableData                = mustLoad("bigtable")
	BlobData                    = mustLoad("blob")
	Check_constraintData        = mustLoad("check_constraint")
	Comp_index_tablesData       = mustLoad("comp_index_tables")
	DatetimetableData           = mustLoad("datetimetable")
	EmptytableData              = mustLoad("emptytable")
	ExplodeData                 = mustLoad("explode")
	Fk_tblData                  = mustLoad("fk_tbl")
	FloattableData              = mustLoad("floattable")
	FooData                     = mustLoad("foo")
	Generated_column_tablesData = mustLoad("generated_column_tables")
	Graph_tablesData            = mustLoad("graph_tables")
	Id_parentData               = mustLoad("id_parent")
	ImdbData                    = mustLoad("imdb")
	Integration_testData        = mustLoad("integration_test")
	Invert_pkData               = mustLoad("invert_pk")
	JoinData                    = mustLoad("join")
	JsontableData               = mustLoad("jsontable")
	KeylessData                 = mustLoad("keyless")
	Keyless_idxData             = mustLoad("keyless_idx")
	LoadtableData               = mustLoad("loadtable")
	MydbData                    = mustLoad("mydb")
	MyhistorytableData          = mustLoad("myhistorytable")
	MytableData                 = mustLoad("mytable")
	Mytable_del_idxData         = mustLoad("mytable_del_idx")
	NewlinetableData            = mustLoad("newlinetable")
	NiltableData                = mustLoad("niltable")
	Null_rangesData             = mustLoad("null_ranges")
	Ordinals_ddlData            = mustLoad("ordinals_ddl")
	OthertableData              = mustLoad("othertable")
	Othertable_del_idxData      = mustLoad("othertable_del_idx")
	Parent_childData            = mustLoad("parent_child")
	PeopleData                  = mustLoad("people")
	Pk_tablesData               = mustLoad("pk_tables")
	Reserved_keywordsData       = mustLoad("reserved_keywords")
	SpatialData                 = mustLoad("spatial")
	SpecialtableData            = mustLoad("specialtable")
	StringandtableData          = mustLoad("stringandtable")
	SysbenchData                = mustLoad("sysbench")
	TabletestData               = mustLoad("tabletest")
	TestdbData                  = mustLoad("testdb")
	TpccData                    = mustLoad("tpcc")
	TpcdsData                   = mustLoad("tpcds")
	TpchData                    = mustLoad("tpch")
	TypestableData              = mustLoad("typestable")
	ViewsData                   = mustLoad("views")
	XyData                      = mustLoad("xy")
)
