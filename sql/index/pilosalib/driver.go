package pilosalib

import (
	"crypto/sha1"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
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

	// ConfigFileName is the extension of an index config file.
	ConfigFileName = "config.yml"

	// ProcessingFileName is the extension of the lock/processing index file.
	ProcessingFileName = ".processing"

	// MappingFileName is the extension of the mapping file.
	MappingFileName = "mapping.db"
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
		root:   root,
		holder: pilosa.NewHolder(),
	}
}

// ID returns the unique name of the driver.
func (*Driver) ID() string {
	return DriverID
}

// Create a new index.
func (d *Driver) Create(db, table, id string, expressions []sql.Expression, config map[string]string) (sql.Index, error) {
	_, err := mkdir(d.root, db, table, id)
	if err != nil {
		return nil, err
	}
	name := indexName(db, table)
	if config == nil {
		config = make(map[string]string)
	}
	exprs := make([]string, len(expressions))
	for i, e := range expressions {
		name := e.String()

		exprs[i] = name
		config[fieldName(id, name)] = name
	}

	cfg := index.NewConfig(db, table, id, exprs, d.ID(), config)
	err = index.WriteConfigFile(d.configFilePath(db, table, id), cfg)
	if err != nil {
		return nil, err
	}

	d.holder.Path = d.pilosaDirPath(db, table)
	idx, err := d.holder.CreateIndexIfNotExists(name, pilosa.IndexOptions{})
	if err != nil {
		return nil, err
	}
	mapping := newMapping(d.mappingFilePath(db, table, id))

	return newPilosaIndex(idx, mapping, cfg), nil
}

// LoadAll loads all indexes for given db and table
func (d *Driver) LoadAll(db, table string) ([]sql.Index, error) {
	var (
		indexes []sql.Index
		errors  []string
		root    = filepath.Join(d.root, db, table)
	)

	d.holder.Path = d.pilosaDirPath(db, table)
	err := d.holder.Open()
	if err != nil {
		return nil, err
	}
	defer d.holder.Close()

	dirs, err := ioutil.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return indexes, nil
		}
		return nil, err
	}
	for _, info := range dirs {
		if info.IsDir() && !strings.HasPrefix(info.Name(), ".") {
			idx, err := d.loadIndex(db, table, info.Name())
			if err != nil {
				if !errCorruptedIndex.Is(err) {
					errors = append(errors, err.Error())
				}
				continue
			}

			indexes = append(indexes, idx)
		}
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

	dir := filepath.Join(d.root, db, table, id)
	config := d.configFilePath(db, table, id)
	if _, err := os.Stat(config); err != nil {
		return nil, errCorruptedIndex.New(dir)
	}

	mapping := d.mappingFilePath(db, table, id)
	processing := d.processingFilePath(db, table, id)
	ok, err := index.ExistsProcessingFile(processing)
	if err != nil {
		return nil, err
	}
	if ok {
		log := logrus.WithFields(logrus.Fields{
			"err":   err,
			"db":    db,
			"table": table,
			"id":    id,
			"dir":   dir,
		})
		log.Warn("could not read index file, index is corrupt and will be deleted")
		if err := os.RemoveAll(dir); err != nil {
			log.Warn("unable to remove corrupted index: " + dir)
		}

		return nil, errCorruptedIndex.New(dir)
	}

	cfg, err := index.ReadConfigFile(config)
	if err != nil {
		return nil, err
	}
	if cfg.Driver(DriverID) == nil {
		return nil, errCorruptedIndex.New(dir)
	}

	return newPilosaIndex(idx, newMapping(mapping), cfg), nil
}

// Save the given index (mapping and bitmap)
func (d *Driver) Save(ctx *sql.Context, i sql.Index, iter sql.IndexKeyValueIter) (err error) {
	var colID uint64
	start := time.Now()

	idx, ok := i.(*pilosaIndex)
	if !ok {
		return errInvalidIndexType.New(i)
	}

	processingFile := d.processingFilePath(idx.Database(), idx.Table(), idx.ID())
	if err = index.CreateProcessingFile(processingFile); err != nil {
		return err
	}

	pilosaIndex := idx.index
	if err = pilosaIndex.Open(); err != nil {
		return err
	}
	defer pilosaIndex.Close()

	d.fields = make([]*pilosa.Field, len(idx.Expressions()))
	for i, e := range idx.Expressions() {
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
	if err := os.RemoveAll(filepath.Join(d.root, i.Database(), i.Table(), i.ID())); err != nil {
		return err
	}

	idx, ok := i.(*pilosaIndex)
	if !ok {
		return errInvalidIndexType.New(i)
	}

	err := idx.index.Open()
	if err != nil {
		return err
	}
	defer idx.index.Close()

	for _, ex := range idx.Expressions() {
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

func fieldName(id string, ex string) string {
	h := sha1.New()
	io.WriteString(h, id)
	io.WriteString(h, ex)
	return fmt.Sprintf("%s-%x", FieldNamePrefix, h.Sum(nil))
}

// mkdir makes an empty index directory (if doesn't exist) and returns a path.
func mkdir(elem ...string) (string, error) {
	path := filepath.Join(elem...)
	return path, os.MkdirAll(path, 0750)
}

func (d *Driver) pilosaDirPath(db, table string) string {
	return filepath.Join(d.root, db, table, "."+DriverID)
}

func (d *Driver) configFilePath(db, table, id string) string {
	return filepath.Join(d.root, db, table, id, ConfigFileName)
}

func (d *Driver) processingFilePath(db, table, id string) string {
	return filepath.Join(d.root, db, table, id, ProcessingFileName)
}

func (d *Driver) mappingFilePath(db, table, id string) string {
	return filepath.Join(d.root, db, table, id, MappingFileName)
}
