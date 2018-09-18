package pilosa

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"io/ioutil"
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
	// IndexNamePrefix the pilosa's indexes prefix.
	IndexNamePrefix = "idx"
	// FieldNamePrefix the pilosa's field prefix.
	FieldNamePrefix = "fld"

	// ConfigFileName is the name of an index config file.
	ConfigFileName = "config.yml"

	// ProcessingFileName is the name of the lock/processing index file.
	ProcessingFileName = ".processing"

	// MappingFileName is the name of the mapping file.
	MappingFileName = "mapping.db"
)

var (
	errCorruptedIndex    = errors.NewKind("the index in %q is corrupted")
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

	// used for saving
	batch struct {
		bitBatches  []*bitBatch
		fields      []*pilosa.Field
		timePilosa  time.Duration
		timeMapping time.Duration
	}

	// Driver implements sql.IndexDriver interface.
	Driver struct {
		root   string
		client *pilosa.Client
	}
)

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
func (d *Driver) Create(db, table, id string, expressions []sql.Expression, config map[string]string) (sql.Index, error) {
	_, err := mkdir(d.root, db, table, id)
	if err != nil {
		return nil, err
	}

	exprs := make([]string, len(expressions))
	for i, e := range expressions {
		exprs[i] = e.String()
	}

	cfg := index.NewConfig(db, table, id, exprs, d.ID(), config)
	err = index.WriteConfigFile(d.configFilePath(db, table, id), cfg)
	if err != nil {
		return nil, err
	}

	return newPilosaIndex(d.mappingFilePath(db, table, id), d.client, cfg), nil
}

// LoadAll loads all indexes for given db and table
func (d *Driver) LoadAll(db, table string) ([]sql.Index, error) {
	var (
		indexes []sql.Index
		errors  []string
		root    = filepath.Join(d.root, db, table)
	)

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

func (d *Driver) loadIndex(db, table, id string) (sql.Index, error) {
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

	idx := newPilosaIndex(mapping, d.client, cfg)
	return idx, nil
}

func (d *Driver) savePartition(
	ctx *sql.Context,
	partition sql.Partition,
	kviter sql.IndexKeyValueIter,
	schema *pilosa.Schema,
	idx *pilosaIndex,
	offset uint64,
	b *batch,
) (uint64, error) {
	var (
		colID uint64
		err   error
	)
	// Create a pilosa index and frame objects in memory
	pilosaIndex := schema.Index(indexName(idx.Database(), idx.Table()))
	if err := d.client.EnsureIndex(pilosaIndex); err != nil {
		return 0, err
	}

	for i, e := range idx.Expressions() {
		name := fieldName(idx.ID(), e, partition)
		field := pilosaIndex.Field(name, pilosa.OptFieldTypeSet(
			pilosa.CacheTypeDefault,
			pilosa.CacheSizeDefault,
		))

		if err := d.client.CreateField(field); err != nil {
			if err == pilosa.ErrFieldExists {
				// make sure we delete the index in every run before inserting,
				// since there may be previous data
				if err := d.client.DeleteField(field); err != nil {
					return 0, errDeletePilosaField.New(field.Name(), err)
				}

				if err := d.client.CreateField(field); err != nil {
					return 0, fmt.Errorf("failed to create field after delete: %s", err)
				}
			} else {
				return 0, fmt.Errorf("failed to create pilosa field: %s", err)
			}
		}

		b.fields[i] = field
		b.bitBatches[i] = newBitBatch(sql.IndexBatchSize)
	}

	// Make sure the index and frames exists on the server
	if err = d.client.SyncSchema(schema); err != nil {
		return 0, fmt.Errorf("unable to sync schema: %s", err)
	}

	// Open mapping in create mode. After finishing the transaction is rolled
	// back unless all goes well and rollback value is changed.
	rollback := true
	idx.mapping.openCreate(true)
	defer func() {
		if rollback {
			idx.mapping.rollback()
		} else {
			e := d.saveMapping(ctx, idx.mapping, colID, false, b)
			if e != nil && err == nil {
				err = e
			}
		}

		idx.mapping.close()
	}()

	for colID = offset; err == nil; colID++ {
		// commit each batch of objects (pilosa and boltdb)
		if colID%sql.IndexBatchSize == 0 && colID != 0 {
			if err = d.saveBatch(ctx, idx.mapping, colID, b); err != nil {
				return 0, err
			}
		}

		select {
		case <-ctx.Context.Done():
			return 0, ctx.Context.Err()

		default:
			var (
				values   []interface{}
				location []byte
			)
			if values, location, err = kviter.Next(); err != nil {
				break
			}

			for i, frm := range b.fields {
				if values[i] == nil {
					continue
				}

				rowID, err := idx.mapping.getRowID(frm.Name(), values[i])
				if err != nil {
					return 0, err
				}

				b.bitBatches[i].Add(rowID, colID)
			}
			err = idx.mapping.putLocation(pilosaIndex.Name(), colID, location)
		}
	}

	if err != nil && err != io.EOF {
		return 0, err
	}

	rollback = false

	err = d.savePilosa(ctx, colID, b)
	if err != nil {
		return 0, err
	}

	return colID - offset, nil
}

// Save the given index (mapping and bitmap)
func (d *Driver) Save(
	ctx *sql.Context,
	i sql.Index,
	iter sql.PartitionIndexKeyValueIter,
) (err error) {
	start := time.Now()

	idx, ok := i.(*pilosaIndex)
	if !ok {
		return errInvalidIndexType.New(i)
	}
	idx.wg.Add(1)
	defer idx.wg.Done()

	var b = batch{
		fields:     make([]*pilosa.Field, len(idx.Expressions())),
		bitBatches: make([]*bitBatch, len(idx.Expressions())),
	}

	ctx.Context, idx.cancel = context.WithCancel(ctx.Context)
	processingFile := d.processingFilePath(idx.Database(), idx.Table(), idx.ID())
	if err = index.CreateProcessingFile(processingFile); err != nil {
		return err
	}

	// Retrieve the pilosa schema
	schema, err := d.client.Schema()
	if err != nil {
		return err
	}

	var rows uint64
	for {
		partition, kviter, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		numRows, err := d.savePartition(ctx, partition, kviter, schema, idx, rows, &b)
		if err != nil {
			return err
		}

		rows += numRows
	}

	if err := iter.Close(); err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"duration": time.Since(start),
		"pilosa":   b.timePilosa,
		"mapping":  b.timeMapping,
		"rows":     rows,
		"id":       i.ID(),
	}).Debugf("finished pilosa indexing")

	return index.RemoveProcessingFile(processingFile)
}

// Delete the given index for all partitions in the iterator.
func (d *Driver) Delete(i sql.Index, partitions sql.PartitionIter) error {
	idx, ok := i.(*pilosaIndex)
	if !ok {
		return errInvalidIndexType.New(i)
	}
	if idx.cancel != nil {
		idx.cancel()
		idx.wg.Wait()
	}

	if err := os.RemoveAll(filepath.Join(d.root, idx.Database(), idx.Table(), idx.ID())); err != nil {
		return err
	}

	index := pilosa.NewIndex(indexName(idx.Database(), idx.Table()))
	if err := d.client.EnsureIndex(index); err != nil {
		return err
	}

	for {
		p, err := partitions.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		fields := index.Fields()
		for _, ex := range idx.Expressions() {
			f, ok := fields[fieldName(idx.ID(), ex, p)]
			if !ok {
				continue
			}

			if err = d.client.DeleteField(f); err != nil {
				return err
			}
		}
	}

	return partitions.Close()
}

func (d *Driver) saveBatch(ctx *sql.Context, m *mapping, colID uint64, b *batch) error {
	err := d.savePilosa(ctx, colID, b)
	if err != nil {
		return err
	}

	return d.saveMapping(ctx, m, colID, true, b)
}

func (d *Driver) savePilosa(ctx *sql.Context, colID uint64, b *batch) error {
	span, _ := ctx.Span("pilosa.Save.bitBatch",
		opentracing.Tag{Key: "cols", Value: colID},
		opentracing.Tag{Key: "fields", Value: len(b.fields)},
	)
	defer span.Finish()

	start := time.Now()

	for i, f := range b.fields {
		err := d.client.ImportField(f, b.bitBatches[i])
		if err != nil {
			span.LogKV("error", err)
			return err
		}

		b.bitBatches[i].Clean()
	}

	b.timePilosa += time.Since(start)

	return nil
}

func (d *Driver) saveMapping(
	ctx *sql.Context,
	m *mapping,
	colID uint64,
	cont bool,
	b *batch,
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

	b.timeMapping += time.Since(start)

	return nil
}

func newBitBatch(size uint64) *bitBatch {
	b := &bitBatch{size: size}
	b.Clean()

	return b
}

func (b *bitBatch) Clean() {
	b.cols = make([]uint64, 0, b.size)
	b.rows = make([]uint64, 0, b.size)
	b.pos = 0
}

func (b *bitBatch) Add(row, col uint64) {
	b.rows = append(b.rows, row)
	b.cols = append(b.cols, col)
}

func (b *bitBatch) NextRecord() (pilosa.Record, error) {
	if b.pos >= uint64(len(b.rows)) {
		return nil, io.EOF
	}

	b.pos++
	return pilosa.Column{
		RowID:    b.rows[b.pos-1],
		ColumnID: b.cols[b.pos-1],
	}, nil
}

func indexName(db, table string) string {
	h := sha1.New()
	io.WriteString(h, db)
	io.WriteString(h, table)

	return fmt.Sprintf("%s-%x", IndexNamePrefix, h.Sum(nil))
}

func fieldName(id string, ex string, p sql.Partition) string {
	h := sha1.New()
	io.WriteString(h, id)
	io.WriteString(h, ex)
	h.Write(p.Key())
	return fmt.Sprintf("%s-%x", FieldNamePrefix, h.Sum(nil))
}

// mkdir makes an empty index directory (if doesn't exist) and returns a path.
func mkdir(elem ...string) (string, error) {
	path := filepath.Join(elem...)
	return path, os.MkdirAll(path, 0750)
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
