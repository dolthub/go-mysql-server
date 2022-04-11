// Copyright 2020-2021 Dolthub, Inc.
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

package benchmark

import (
	"encoding/csv"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"testing"

	sqle "github.com/dolthub/go-mysql-server"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

var scriptsPath = "../_scripts/tpc-h/"

func BenchmarkTpch(b *testing.B) {
	b.Log("generating data")
	if err := genData(b); err != nil {
		b.Fatal(err)
	}

	b.Log("generating database")
	db, err := genDB(b)
	if err != nil {
		b.Fatal(err)
	}

	e := sqle.NewDefault(sql.NewDatabaseProvider(db))
	b.ResetTimer()

	if err := executeQueries(b, e); err != nil {
		b.Fatal(err)
	}
}

func executeQueries(b *testing.B, e *sqle.Engine) error {
	base := path.Join(scriptsPath, "queries")
	infos, err := ioutil.ReadDir(base)
	if err != nil {
		return err
	}

	ctx := sql.NewEmptyContext()

	for _, info := range infos {
		if info.IsDir() {
			continue
		}

		b.Run(info.Name(), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				query, err := ioutil.ReadFile(path.Join(base, info.Name()))
				if err != nil {
					b.Fatal(err)
				}

				_, iter, err := e.Query(ctx, string(query))
				if err != nil {
					b.Fatal(err)
				}

				for {
					_, err = iter.Next(ctx)
					if err == io.EOF {
						break
					}

					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}

	return nil
}

func genDB(b *testing.B) (sql.Database, error) {
	db := memory.NewDatabase("tpch")

	for _, m := range tpchTableMetadata {
		b.Log("generating table", m.name)
		t := memory.NewTable(m.name, m.schema, db.GetForeignKeyCollection())
		if err := insertDataToTable(m.name, t, len(m.schema.Schema)); err != nil {
			return nil, err
		}

		db.AddTable(m.name, t)
	}

	return db, nil
}

func insertDataToTable(name string, t *memory.Table, columnCount int) error {
	f, err := os.Open(name + ".tbl")
	if err != nil {
		return err
	}
	r := csv.NewReader(f)
	r.Comma = '|'
	r.FieldsPerRecord = columnCount + 1 // trailing separator

	for {
		d, err := r.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		row, err := rawToRow(d, t.Schema())
		if err != nil {
			return err
		}

		if err := t.Insert(sql.NewEmptyContext(), row); err != nil {
			return err
		}
	}

	return nil
}

func rawToRow(d []string, s sql.Schema) (sql.Row, error) {
	var parsed []interface{}
	for i, c := range s {
		pv, err := c.Type.Convert(d[i])
		if err != nil {
			return nil, err
		}

		parsed = append(parsed, pv)
	}

	return sql.NewRow(parsed...), nil
}

func genData(b *testing.B) error {
	b.Log("Creating data files...")
	cmd := exec.Command(
		filepath.Join(scriptsPath, "dbgen"),
		"-b",
		filepath.Join(scriptsPath, "dists.dss"),
		"-vf",
		// TODO parametrize
		"-s0.1",
	)

	return cmd.Run()
}
