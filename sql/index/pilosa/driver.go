package pilosa

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	pilosa "github.com/pilosa/go-pilosa"
	errors "gopkg.in/src-d/go-errors.v0"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/index"
)

const (
	// DriverID the unique name of the pilosa driver.
	DriverID = "pilosa"
	// IndexNamePrefix the pilosa's indexes prefix
	IndexNamePrefix = "idx"
	// FrameNamePrefix the pilosa's frames prefix
	FrameNamePrefix = "frm"
)

// Driver implements sql.IndexDriver interface.
type Driver struct {
	root   string
	client *pilosa.Client
}

// NewDriver returns a new instance of pilosa.Driver
// which satisfies sql.IndexDriver interface
func NewDriver(root string, client *pilosa.Client) *Driver {
	return &Driver{
		root:   root,
		client: client,
	}
}

// NewIndexDriver returns a default instance of pilosa.Driver
func NewIndexDriver(root string) sql.IndexDriver {
	return NewDriver(root, pilosa.DefaultClient())
}

// ID returns the unique name of the driver.
func (*Driver) ID() string {
	return DriverID
}

// Create a new index.
func (d *Driver) Create(db, table, id string, expr []sql.ExpressionHash, config map[string]string) (sql.Index, error) {
	path, err := mkdir(d.root, db, table, id)
	if err != nil {
		return nil, err
	}

	cfg := index.NewConfig(db, table, id, expr, d.ID(), config)
	err = index.WriteConfigFile(path, cfg)
	if err != nil {
		return nil, err
	}

	return &pilosaIndex{
		path:        path,
		client:      d.client,
		db:          db,
		table:       table,
		id:          id,
		expressions: expr,
		mapping:     newMapping(path),
	}, nil
}

// LoadAll loads all indexes for given db and table
func (d *Driver) LoadAll(db, table string) ([]sql.Index, error) {
	root := filepath.Join(d.root, db, table)

	var (
		indexes []sql.Index
		errors  []string
		err     error
	)
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if path == root && !os.IsNotExist(err) {
				errors = append(errors, err.Error())
			}
			return filepath.SkipDir
		}

		if info.IsDir() && path != root && info.Name() != "." && info.Name() != ".." {
			cfg, err := index.ReadConfigFile(path)
			if err != nil {
				errors = append(errors, err.Error())
				return filepath.SkipDir
			}

			indexes = append(indexes, &pilosaIndex{
				path:        path,
				client:      d.client,
				db:          cfg.DB,
				table:       cfg.Table,
				id:          cfg.ID,
				expressions: cfg.ExpressionHashes(),
			})
		}

		return nil
	})

	if len(errors) > 0 {
		err = fmt.Errorf(strings.Join(errors, "\n"))
	}
	return indexes, err
}

var errInvalidIndexType = errors.NewKind("expecting a pilosa index, instead got %T")

// Save the given index (mapping and bitmap)
func (d *Driver) Save(ctx context.Context, i sql.Index, iter sql.IndexKeyValueIter) error {
	idx, ok := i.(*pilosaIndex)
	if !ok {
		return errInvalidIndexType.New(i)
	}

	// Retrieve the pilosa schema
	schema, err := d.client.Schema()
	if err != nil {
		return err
	}
	// Create a pilosa index and frame objects in memory
	index, err := schema.Index(indexName(idx.Database(), idx.Table(), idx.ID()))
	if err != nil {
		return err
	}

	frames := make([]*pilosa.Frame, len(idx.ExpressionHashes()))
	for i, e := range idx.ExpressionHashes() {
		frames[i], err = index.Frame(frameName(e))
		if err != nil {
			return err
		}
	}

	// Make sure the index and frames exists on the server
	err = d.client.SyncSchema(schema)
	if err != nil {
		return err
	}

	idx.mapping.open()
	defer idx.mapping.close()

	for colID := uint64(0); err == nil; colID++ {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			var (
				values   []interface{}
				location []byte
			)
			values, location, err = iter.Next()
			if err != nil {
				break
			}

			for i, frm := range frames {
				rowID, err := idx.mapping.getRowID(frm.Name(), values[i])
				if err != nil {
					return err
				}

				resp, err := d.client.Query(frm.SetBit(rowID, colID))
				if err != nil {
					return err
				}
				if !resp.Success {
					return errPilosaQuery.New(resp.ErrorMessage)
				}
			}
			err = idx.mapping.putLocation(index.Name(), colID, location)
		}
	}

	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

// Delete the index with the given path.
func (d *Driver) Delete(idx sql.Index) error {
	path := filepath.Join(d.root, idx.Database(), idx.Table(), idx.ID())
	os.RemoveAll(path)

	index, err := pilosa.NewIndex(indexName(idx.Database(), idx.Table(), idx.ID()))
	if err != nil {
		return err
	}
	return d.client.DeleteIndex(index)
}

func indexName(db, table, id string) string {
	h := sha1.New()
	io.WriteString(h, db)
	io.WriteString(h, table)
	io.WriteString(h, id)

	return fmt.Sprintf("%s-%x", IndexNamePrefix, h.Sum(nil))
}

func frameName(ex sql.ExpressionHash) string {
	return fmt.Sprintf("%s-%x", FrameNamePrefix, ex)
}

// mkdir makes an empty index directory (if doesn't exist) and returns a path.
func mkdir(elem ...string) (string, error) {
	path := filepath.Join(elem...)
	return path, os.MkdirAll(path, 0750)
}
