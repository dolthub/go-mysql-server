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

	"gopkg.in/src-d/go-mysql-server.v0"

	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
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

	e := sqle.NewDefault()
	e.AddDatabase(db)
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
					_, err = iter.Next()
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
	db := mem.NewDatabase("tpch")

	for _, m := range tpchTableMetadata {
		b.Log("generating table", m.name)
		t := mem.NewTable(m.name, m.schema)
		if err := insertDataToTable(m.name, t, len(m.schema)); err != nil {
			return nil, err
		}

		db.AddTable(m.name, t)
	}

	return db, nil
}

func insertDataToTable(name string, t *mem.Table, columnCount int) error {
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
