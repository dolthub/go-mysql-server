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

	e := sqle.New()
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

	for _, info := range infos {
		if info.IsDir() {
			continue
		}

		b.Run(info.Name(), func(b *testing.B) {
			skip, msg := shouldSkip(info.Name())
			if skip {
				b.Skip(msg)
			}

			for n := 0; n < b.N; n++ {
				query, err := ioutil.ReadFile(path.Join(base, info.Name()))
				if err != nil {
					b.Fatal(err)
				}

				_, iter, err := e.Query(string(query))
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

func shouldSkip(name string) (skip bool, msg string) {
	skip = true

	switch name {
	case "16.sql", "17.sql":
		msg = "Unsupported syntax: " +
			"&sqlparser.AndExpr{Left:(*sqlparser.AndExpr)(0xc42899c480), Right:(*sqlparser.ComparisonExpr)(0xc4289c4340)}"
	case "19.sql":
		msg = "Unsupported syntax: " +
			"&sqlparser.OrExpr{Left:(*sqlparser.OrExpr)(0xc42899c960), Right:(*sqlparser.ParenExpr)(0xc449f96f40)}"
	case "22.sql":
		msg = "SUBSTRING function not implemented"
	case "8.sql", "7.sql", "9.sql":
		msg = "YEAR function not implemented"
	case "1.sql", "3.sql", "4.sql", "5.sql", "6.sql", "10.sql", "12.sql", "14.sql", "20.sql":
		msg = "Date type not supported"
	case "18.sql", "2.sql", "21.sql", "11.sql":
		msg = "unsupported feature: more than 2 tables in JOIN"
	case "13.sql":
		msg = "Aliased tables not supported"
	case "15.sql":
		msg = "Views not supported"
	default:
		skip = false
	}

	return
}

func genDB(b *testing.B) (sql.Database, error) {
	db := mem.NewDatabase("tpch")

	for _, m := range tpchTableMetadata {
		b.Log("generating table", m.name)
		t := mem.NewTable(m.name, m.schema)
		if err := insertDataToTable(t, len(m.schema)); err != nil {
			return nil, err
		}

		db.AddTable(m.name, t)
	}

	return db, nil
}

func insertDataToTable(t *mem.Table, columnCount int) error {
	f, err := os.Open(t.Name() + ".tbl")
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

		if err := t.Insert(row); err != nil {
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
