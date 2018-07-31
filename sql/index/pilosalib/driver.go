package pilosalib

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	pilosa "github.com/pilosa/pilosa"
	"github.com/sirupsen/logrus"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/index"
)

const (
	// DriverID the unique name of the pilosa driver.
	DriverID = "pilosalib"

	// IndexNamePrefix the pilosa's indexes prefix
	IndexNamePrefix = "idx"

	// FieldNamePrefix the pilosa's field prefix
	FieldNamePrefix = "fld"

	// ConfigFileExt is the extension of an index config file.
	ConfigFileExt = ".cfg"

	// ProcessingFileExt is the extension of the lock/processing index file.
	ProcessingFileExt = ".processing"

	// MappingFileExt is the extension of the mapping file.
	MappingFileExt = ".map"
)

var (
	errCorruptedIndex    = errors.NewKind("the index db: %s, table: %s, id: %s is corrupted")
	errLoadingIndex      = errors.NewKind("cannot load pilosa index: %s")
	errInvalidIndexType  = errors.NewKind("expecting a pilosa index, instead got %T")
	errDeletePilosaField = errors.NewKind("error deleting pilosa field %s: %s")
)

type (
	bitBatch struct {
		size uint64
		rows []uint64
		cols []uint64
		pos  uint64
	}

	// Driver implements sql.IndexDriver interface.
	Driver struct {
		root   string
		holder *pilosa.Holder

		// used for saving
		bitBatches  []*bitBatch
		fields      []*pilosa.Field
		timePilosa  time.Duration
		timeMapping time.Duration
	}
)

// NewDriver returns a new instance of pilosalib.Driver
// which satisfies sql.IndexDriver interface
func NewDriver(root string) *Driver {
	return &Driver{
		root:   filepath.Join(root, DriverID),
		holder: pilosa.NewHolder(),
	}
}

// ID returns the unique name of the driver.
func (*Driver) ID() string {
	return DriverID
}

// Create a new index.
func (d *Driver) Create(db, table, id string, expr []sql.ExpressionHash, config map[string]string) (sql.Index, error) {
	root, err := mkdir(d.root, db, table)
	if err != nil {
		return nil, err
	}
	name := indexName(db, table)

	if config == nil {
		config = make(map[string]string)
	}

	config["index"] = name
	for _, e := range expr {
		config[hex.EncodeToString(e)] = fieldName(id, e)
	}
	cfg := index.NewConfig(db, table, id, expr, d.ID(), config)
	err = index.WriteConfigFile(d.configFileName(db, table, id), cfg)
	if err != nil {
		return nil, err
	}

	d.holder.Path = root
	idx, err := d.holder.CreateIndexIfNotExists(name, pilosa.IndexOptions{})
	if err != nil {
		return nil, err
	}
	mapping := newMapping(d.mappingFileName(db, table, id))

	return newPilosaIndex(idx, mapping, cfg), nil
}

// LoadAll loads all indexes for given db and table
func (d *Driver) LoadAll(db, table string) ([]sql.Index, error) {
	var (
		indexes []sql.Index
		errors  []string
	)

	d.holder.Path = filepath.Join(d.root, db, table)
	err := d.holder.Open()
	if err != nil {
		return nil, err
	}
	defer d.holder.Close()

	configfiles, err := filepath.Glob(filepath.Join(d.holder.Path, "*") + ConfigFileExt)
	if err != nil {
		return nil, err
	}
	for _, path := range configfiles {
		filename := filepath.Base(path)
		id := filename[:len(filename)-len(ConfigFileExt)]
		idx, err := d.loadIndex(db, table, id)
		if err != nil {
			if errLoadingIndex.Is(err) {
				return nil, err
			}
			if !errCorruptedIndex.Is(err) {
				errors = append(errors, err.Error())
			}
			continue
		}
		indexes = append(indexes, idx)
	}

	if len(errors) > 0 {
		return nil, fmt.Errorf(strings.Join(errors, "\n"))
	}
	return indexes, nil
}

func (d *Driver) loadIndex(db, table, id string) (*pilosaIndex, error) {
	name := indexName(db, table)
	idx := d.holder.Index(name)
	if idx == nil {
		return nil, errLoadingIndex.New(name)
	}

	processing := d.processingFileName(db, table, id)
	ok, err := index.ExistsProcessingFile(processing)
	if err != nil {
		return nil, err
	}
	if ok {
		log := logrus.WithFields(logrus.Fields{
			"db":    db,
			"table": table,
			"id":    id,
		})
		log.Warn("could not read index file, index is corrupt and will be deleted")
		d.removeResources(db, table, id)
		return nil, errCorruptedIndex.New(id)
	}

	cfg, err := index.ReadConfigFile(d.configFileName(db, table, id))
	if err != nil {
		return nil, err
	}

	mapping := newMapping(d.mappingFileName(db, table, id))
	return newPilosaIndex(idx, mapping, cfg), nil
}

// Save the given index (mapping and bitmap)
func (d *Driver) Save(ctx *sql.Context, i sql.Index, iter sql.IndexKeyValueIter) (err error) {
	var colID uint64
	start := time.Now()

	idx, ok := i.(*pilosaIndex)
	if !ok {
		return errInvalidIndexType.New(i)
	}

	processingFile := d.processingFileName(idx.Database(), idx.Table(), idx.ID())
	if err = index.CreateProcessingFile(processingFile); err != nil {
		return err
	}

	pilosaIndex := idx.index
	if err = pilosaIndex.Open(); err != nil {
		return err
	}
	defer pilosaIndex.Close()

	d.fields = make([]*pilosa.Field, len(idx.ExpressionHashes()))
	for i, e := range idx.ExpressionHashes() {
		name := fieldName(idx.ID(), e)
		pilosaIndex.DeleteField(name)
		field, err := pilosaIndex.CreateField(name, pilosa.OptFieldTypeDefault())
		if err != nil {
			return err
		}
		d.fields[i] = field
	}

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

	d.bitBatches = make([]*bitBatch, len(d.fields))
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

			for i, field := range d.fields {
				if values[i] == nil {
					continue
				}

				rowID, err := idx.mapping.getRowID(field.Name(), values[i])
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

	return index.RemoveProcessingFile(processingFile)
}

// Delete the index with the given path.
func (d *Driver) Delete(i sql.Index) error {
	idx, ok := i.(*pilosaIndex)
	if !ok {
		return errInvalidIndexType.New(i)
	}

	d.removeResources(idx.Database(), idx.Table(), idx.ID())

	err := idx.index.Open()
	if err != nil {
		return err
	}
	defer idx.index.Close()

	for _, ex := range idx.ExpressionHashes() {
		name := fieldName(idx.ID(), ex)
		field := idx.index.Field(name)
		if field == nil {
			continue
		}

		if err = idx.index.DeleteField(name); err != nil {
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
		opentracing.Tag{Key: "frames", Value: len(d.fields)},
	)
	defer span.Finish()

	start := time.Now()

	for i, frm := range d.fields {
		err := frm.Import(d.bitBatches[i].rows, d.bitBatches[i].cols, nil)
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

func newBitBatch(size uint64) *bitBatch {
	b := &bitBatch{size: size}
	b.Clean()

	return b
}

func (b *bitBatch) Clean() {
	b.rows = make([]uint64, 0, b.size)
	b.rows = make([]uint64, 0, b.size)
	b.pos = 0
}

func (b *bitBatch) Add(row, col uint64) {
	b.rows = append(b.rows, row)
	b.cols = append(b.cols, col)
}

func (b *bitBatch) NextRecord() (uint64, uint64, error) {
	if b.pos >= uint64(len(b.rows)) {
		return 0, 0, io.EOF
	}

	b.pos++
	return b.rows[b.pos-1], b.cols[b.pos-1], nil
}

func indexName(db, table string) string {
	h := sha1.New()
	io.WriteString(h, db)
	io.WriteString(h, table)

	return fmt.Sprintf("%s-%x", IndexNamePrefix, h.Sum(nil))
}

func fieldName(id string, ex sql.ExpressionHash) string {
	h := sha1.New()
	io.WriteString(h, id)
	h.Write(ex)
	return fmt.Sprintf("%s-%x", FieldNamePrefix, h.Sum(nil))
}

// mkdir makes an empty index directory (if doesn't exist) and returns a path.
func mkdir(elem ...string) (string, error) {
	path := filepath.Join(elem...)
	return path, os.MkdirAll(path, 0750)
}

func (d *Driver) configFileName(db, table, id string) string {
	return filepath.Join(d.root, db, table, id) + ConfigFileExt
}

func (d *Driver) processingFileName(db, table, id string) string {
	return filepath.Join(d.root, db, table, id) + ProcessingFileExt
}

func (d *Driver) mappingFileName(db, table, id string) string {
	return filepath.Join(d.root, db, table, id) + MappingFileExt
}

func (d *Driver) removeResources(db, table, id string) {
	if err := os.RemoveAll(d.configFileName(db, table, id)); err != nil {
		log.Error(err)
	}

	if err := os.RemoveAll(d.processingFileName(db, table, id)); err != nil {
		log.Error(err)
	}

	if err := os.RemoveAll(d.mappingFileName(db, table, id)); err != nil {
		log.Error(err)
	}
}
