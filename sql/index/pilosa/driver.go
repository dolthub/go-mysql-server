package pilosa

import (
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	pilosa "github.com/pilosa/go-pilosa"
	"github.com/sirupsen/logrus"
	errors "gopkg.in/src-d/go-errors.v1"
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

var (
	errCorruptedIndex    = errors.NewKind("the index in %q is corrupted")
	errInvalidIndexType  = errors.NewKind("expecting a pilosa index, instead got %T")
	errDeletePilosaFrame = errors.NewKind("error deleting pilosa frame %s: %s")
)

// Driver implements sql.IndexDriver interface.
type Driver struct {
	root   string
	client *pilosa.Client

	// used for saving
	bitBatches  []*bitBatch
	frames      []*pilosa.Frame
	timePilosa  time.Duration
	timeMapping time.Duration
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

	return newPilosaIndex(path, d.client, cfg), nil
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
			if path != root || !os.IsNotExist(err) {
				errors = append(errors, err.Error())
			}
			return filepath.SkipDir
		}

		if info.IsDir() && path != root && info.Name() != "." && info.Name() != ".." {
			idx, err := d.loadIndex(path)
			if err != nil {
				if !errCorruptedIndex.Is(err) {
					errors = append(errors, err.Error())
				}

				return filepath.SkipDir
			}

			indexes = append(indexes, idx)
		}

		return nil
	})

	if len(errors) > 0 {
		err = fmt.Errorf(strings.Join(errors, "\n"))
	}
	return indexes, err
}

func (d *Driver) loadIndex(path string) (sql.Index, error) {
	ok, err := index.ExistsProcessingFile(path)
	if err != nil {
		return nil, err
	}

	if ok {
		log := logrus.WithFields(logrus.Fields{
			"err":  err,
			"path": path,
		})
		log.Warn("could not read index file, index is corrupt and will be deleted")

		if err := os.RemoveAll(path); err != nil {
			log.Warn("unable to remove folder of corrupted index")
		}

		return nil, errCorruptedIndex.New(path)
	}

	cfg, err := index.ReadConfigFile(path)
	if err != nil {
		return nil, err
	}

	idx := newPilosaIndex(path, d.client, cfg)
	return idx, nil
}

// Save the given index (mapping and bitmap)
func (d *Driver) Save(
	ctx *sql.Context,
	i sql.Index,
	iter sql.IndexKeyValueIter,
) (err error) {
	var colID uint64
	start := time.Now()

	idx, ok := i.(*pilosaIndex)
	if !ok {
		return errInvalidIndexType.New(i)
	}

	path, err := mkdir(d.root, i.Database(), i.Table(), i.ID())
	if err != nil {
		return err
	}

	if err = index.CreateProcessingFile(path); err != nil {
		return err
	}

	// Retrieve the pilosa schema
	schema, err := d.client.Schema()
	if err != nil {
		return err
	}

	// Create a pilosa index and frame objects in memory
	pilosaIndex, err := schema.Index(indexName(idx.Database(), idx.Table()))
	if err != nil {
		return err
	}

	d.frames = make([]*pilosa.Frame, len(idx.ExpressionHashes()))
	for i, e := range idx.ExpressionHashes() {
		frm, err := pilosaIndex.Frame(frameName(idx.ID(), e))
		if err != nil {
			return err
		}
		// make sure we delete the index in every run before inserting, since there may
		// be previous data
		if err = d.client.DeleteFrame(frm); err != nil {
			return errDeletePilosaFrame.New(frm.Name(), err)
		}

		d.frames[i] = frm
	}

	// Make sure the index and frames exists on the server
	err = d.client.SyncSchema(schema)
	if err != nil {
		return err
	}

	// Open mapping in create mode. After finishing the transaction is rolled
	// back unless all goes well and rollback value is changed.
	rollback := true
	idx.mapping.openCreate(true)
	defer func() {
		if rollback {
			idx.mapping.rollback()
		} else {
			e := d.saveMapping(ctx, idx.mapping, colID, false)
			if e != nil && err == nil {
				err = e
			}
		}

		idx.mapping.close()
	}()

	d.bitBatches = make([]*bitBatch, len(d.frames))
	for i := range d.bitBatches {
		d.bitBatches[i] = newBitBatch(sql.IndexBatchSize)
	}

	for colID = uint64(0); err == nil; colID++ {
		// commit each batch of objects (pilosa and boltdb)
		if colID%sql.IndexBatchSize == 0 && colID != 0 {
			d.saveBatch(ctx, idx.mapping, colID)
		}

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

			for i, frm := range d.frames {
				if values[i] == nil {
					continue
				}

				rowID, err := idx.mapping.getRowID(frm.Name(), values[i])
				if err != nil {
					return err
				}

				d.bitBatches[i].Add(rowID, colID)
			}
			err = idx.mapping.putLocation(pilosaIndex.Name(), colID, location)
		}
	}

	if err != nil && err != io.EOF {
		return err
	}

	rollback = false

	err = d.savePilosa(ctx, colID)
	if err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"duration": time.Since(start),
		"pilosa":   d.timePilosa,
		"mapping":  d.timeMapping,
		"rows":     colID,
		"id":       i.ID(),
	}).Debugf("finished pilosa indexing")

	return index.RemoveProcessingFile(path)
}

// Delete the index with the given path.
func (d *Driver) Delete(idx sql.Index) error {
	path := filepath.Join(d.root, idx.Database(), idx.Table(), idx.ID())
	if err := os.RemoveAll(path); err != nil {
		return err
	}

	index, err := pilosa.NewIndex(indexName(idx.Database(), idx.Table()))
	if err != nil {
		return err
	}

	frames := index.Frames()
	for _, ex := range idx.ExpressionHashes() {
		frm, ok := frames[frameName(idx.ID(), ex)]
		if !ok {
			continue
		}

		if err = d.client.DeleteFrame(frm); err != nil {
			return err
		}
	}
	return nil
}

func (d *Driver) saveBatch(ctx *sql.Context, m *mapping, colID uint64) error {
	err := d.savePilosa(ctx, colID)
	if err != nil {
		return err
	}

	return d.saveMapping(ctx, m, colID, true)
}

func (d *Driver) savePilosa(ctx *sql.Context, colID uint64) error {
	span, _ := ctx.Span("pilosa.Save.bitBatch",
		opentracing.Tag{Key: "cols", Value: colID},
		opentracing.Tag{Key: "frames", Value: len(d.frames)},
	)
	defer span.Finish()

	start := time.Now()

	for i, frm := range d.frames {
		err := d.client.ImportFrame(frm, d.bitBatches[i])
		if err != nil {
			span.LogKV("error", err)
			return err
		}

		d.bitBatches[i].Clean()
	}

	d.timePilosa += time.Since(start)

	return nil
}

func (d *Driver) saveMapping(
	ctx *sql.Context,
	m *mapping,
	colID uint64,
	cont bool,
) error {
	span, _ := ctx.Span("pilosa.Save.mapping",
		opentracing.Tag{Key: "cols", Value: colID},
		opentracing.Tag{Key: "continues", Value: cont},
	)
	defer span.Finish()

	start := time.Now()

	err := m.commit(cont)
	if err != nil {
		span.LogKV("error", err)
		return err
	}

	d.timeMapping += time.Since(start)

	return nil
}

type bitBatch struct {
	size uint64
	bits []pilosa.Bit
	pos  uint64
}

func newBitBatch(size uint64) *bitBatch {
	b := &bitBatch{size: size}
	b.Clean()

	return b
}

func (b *bitBatch) Clean() {
	b.bits = make([]pilosa.Bit, 0, b.size)
	b.pos = 0
}

func (b *bitBatch) Add(row, col uint64) {
	b.bits = append(b.bits, pilosa.Bit{
		RowID:    row,
		ColumnID: col,
	})
}

func (b *bitBatch) NextRecord() (pilosa.Record, error) {
	if b.pos >= uint64(len(b.bits)) {
		return nil, io.EOF
	}

	b.pos++
	return b.bits[b.pos-1], nil
}

func indexName(db, table string) string {
	h := sha1.New()
	io.WriteString(h, db)
	io.WriteString(h, table)

	return fmt.Sprintf("%s-%x", IndexNamePrefix, h.Sum(nil))
}

func frameName(id string, ex sql.ExpressionHash) string {
	h := sha1.New()
	io.WriteString(h, id)
	h.Write(ex)
	return fmt.Sprintf("%s-%x", FrameNamePrefix, h.Sum(nil))
}

// mkdir makes an empty index directory (if doesn't exist) and returns a path.
func mkdir(elem ...string) (string, error) {
	path := filepath.Join(elem...)
	return path, os.MkdirAll(path, 0750)
}
