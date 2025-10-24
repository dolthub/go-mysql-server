// Copyright 2025 Dolthub, Inc.
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

package plan

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	vquery "github.com/dolthub/vitess/go/vt/proto/query"
)

// https://github.com/MariaDB/server/blob/11.6/sql/log_event.h#L608-L750
const (
	eventUnknown           = 0
	eventQuery             = 2
	eventFormatDescription = 15
	eventTableMap          = 19
	eventWriteRowsV0       = 20
	eventUpdateRowsV0      = 21
	eventDeleteRowsV0      = 22
	eventWriteRowsV1       = 23
	eventUpdateRowsV1      = 24
	eventDeleteRowsV1      = 25
	eventWriteRowsV2       = 30
	eventUpdateRowsV2      = 31
	eventDeleteRowsV2      = 32
	eventMariaAnnotateRows = 160
	eventMariaGTIDList     = 163
)

const (
	eventTypeOffset   = 4
	eventLengthOffset = 9
	eventHeaderSize   = 19
)

// binlogSess holds state that persists across multiple BINLOG statements. The format field stores binlog
// format metadata from FORMAT_DESCRIPTION_EVENT. The tableMapsById field maps table IDs to table metadata from
// TABLE_MAP events. Table mappings are cleared when processing a  new binlog file (detected by
// FORMAT_DESCRIPTION_EVENT) or when receiving the special table ID 0xFFFFFF, because table IDs are only unique within
// a single binlog file and can be reused across files.
//
// TODO: This currently uses a single global state shared by all connections, which is sufficient for now. This can be
// refactored to use a BinlogSessionManager (similar to PreparedDataCache) that tracks state per connection ID with
// a map[uint32]*binlogSession protected by sync.RWMutex, and add DeleteSessionData(connID) cleanup via
// Engine.CloseSession. MariaDB stores this state per-connection at thd->rgi_fake->m_table_map.
// See https://github.com/MariaDB/server/blob/11.4/sql/sql_binlog.cc#L270-L271
// See https://github.com/MariaDB/server/blob/11.4/sql/rpl_rli.h#L811
var binlogSess = &struct {
	format        *mysql.BinlogFormat
	tableMapsById map[uint64]*mysql.TableMap
}{
	tableMapsById: make(map[uint64]*mysql.TableMap),
}

// Binlog executes the BINLOG statement, which replays binary log events. Binary log events are the
// fundamental units of MySQL replication, recording database changes in a binary format for efficiency.
// Tools like mysqldump, mysqlbinlog, and mariadb-binlog read these binary events from log files and
// output them as base64-encoded BINLOG statements for replay.
//
// The base64 string is split by newlines and decoded into a buffer of raw binlog events. Each event
// begins with a 19-byte header containing the event type at byte 4 and event length at bytes 9-12.
// The buffer is processed sequentially, dispatching each event to a handler based on its type.
//
// FORMAT_DESCRIPTION_EVENT stores binlog format metadata in session state. This metadata includes
// header sizes for each event type and the checksum algorithm (OFF, CRC32, or UNDEF). Subsequent
// events require this metadata to parse their headers and determine whether checksums are present.
//
// TABLE_MAP_EVENT creates a mapping from a table ID to the database name, table name, and column
// metadata. Row events reference tables by ID rather than name for encoding efficiency. The mapping
// is stored in session state for use by subsequent row events within the same connection.
//
// WRITE_ROWS_EVENT, UPDATE_ROWS_EVENT, and DELETE_ROWS_EVENT contain binary-encoded row data.
// Before parsing row data, any CRC32 checksum appended to the event must be stripped. Checksums
// verify data integrity during network transmission and disk storage but are not part of the event
// payload structure. The Vitess mysql.CellValue function decodes each column value based on its
// MySQL type code and metadata from the TABLE_MAP event. Decoded values are converted to
// Go-MySQL-Server types using the column's Type.Convert method.
//
// See https://dev.mysql.com/doc/refman/8.4/en/binlog.html for the BINLOG statement specification.
type Binlog struct {
	Base64Str string
	Catalog   sql.Catalog
}

var _ sql.Node = (*Binlog)(nil)

// NewBinlog creates a new Binlog node.
func NewBinlog(base64Str string) *Binlog {
	return &Binlog{
		Base64Str: base64Str,
	}
}

// getTableMap returns the table ID to table metadata map populated by TABLE_MAP events.
func (b *Binlog) getTableMap(ctx *sql.Context) map[uint64]*mysql.TableMap {
	return binlogSess.tableMapsById
}

// getBinlogFormat returns the binlog format metadata, or nil if no FORMAT_DESCRIPTION_EVENT has been processed.
func (b *Binlog) getBinlogFormat(ctx *sql.Context) *mysql.BinlogFormat {
	return binlogSess.format
}

// setBinlogFormat stores the binlog format metadata for use by subsequent events.
func (b *Binlog) setBinlogFormat(ctx *sql.Context, format *mysql.BinlogFormat) {
	binlogSess.format = format
}

// RowIter decodes the base64 string into binlog events, then processes each event to update
// session state or execute DML operations.
func (b *Binlog) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	// Tools like mariadb-binlog output one base64 block per line
	var decoded []byte

	lines := strings.Split(b.Base64Str, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		block, err := base64.StdEncoding.DecodeString(line)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64 binlog data: %w", err)
		}

		decoded = append(decoded, block...)
	}

	offset := 0
	for offset < len(decoded) {
		if offset+eventHeaderSize > len(decoded) {
			return nil, fmt.Errorf("incomplete event header at offset %d", offset)
		}

		eventType := decoded[offset+eventTypeOffset]
		eventLength := binary.LittleEndian.Uint32(decoded[offset+eventLengthOffset : offset+eventLengthOffset+4])

		if offset+int(eventLength) > len(decoded) {
			return nil, fmt.Errorf("incomplete event at offset %d: event length %d exceeds buffer", offset, eventLength)
		}

		eventData := decoded[offset : offset+int(eventLength)]

		if err := b.processEvent(ctx, eventType, eventData); err != nil {
			return nil, fmt.Errorf("error processing event type %d: %w", eventType, err)
		}

		offset += int(eventLength)
	}

	return sql.RowsToRowIter(), nil
}

// processEvent dispatches a binlog event to its handler based on event type.
func (b *Binlog) processEvent(ctx *sql.Context, eventType byte, eventData []byte) error {
	// MariaDB format is backward compatible with MySQL events
	event := mysql.NewMariadbBinlogEvent(eventData)

	switch eventType {
	case eventFormatDescription:
		parsedFormat, err := event.Format()
		if err != nil {
			return fmt.Errorf("failed to parse FORMAT_DESCRIPTION_EVENT: %w", err)
		}
		b.setBinlogFormat(ctx, &parsedFormat)

		// Clear table maps when seeing FORMAT_DESCRIPTION_EVENT. This event appears at position 4 of each
		// new binlog file, and table IDs from the previous file are no longer valid since they can be
		// reused in the new file for different tables.
		tableMapCache := b.getTableMap(ctx)
		for k := range tableMapCache {
			delete(tableMapCache, k)
		}

		ctx.GetLogger().Tracef("Updated binlog format: version=%d, checksum=%d, headerSizes=%d",
			parsedFormat.FormatVersion, parsedFormat.ChecksumAlgorithm, len(parsedFormat.HeaderSizes))
		return nil

	case eventMariaAnnotateRows:
		// Contains the SQL query text that generated subsequent row events
		ctx.GetLogger().Tracef("Processing MARIA_ANNOTATE_ROWS_EVENT")
		return nil

	case eventMariaGTIDList:
		ctx.GetLogger().Tracef("Processing MARIA_GTID_LIST_EVENT")
		return nil

	case eventTableMap:
		format := b.getBinlogFormat(ctx)
		if format == nil {
			return fmt.Errorf("no binlog format available - FORMAT_DESCRIPTION_EVENT must be processed first")
		}

		// Checksums verify data integrity but are not part of the event payload structure
		if format.ChecksumAlgorithm != mysql.BinlogChecksumAlgOff && format.ChecksumAlgorithm != mysql.BinlogChecksumAlgUndef {
			var err error
			event, _, err = event.StripChecksum(*format)
			if err != nil {
				return fmt.Errorf("failed to strip checksum from TABLE_MAP_EVENT: %w", err)
			}
		}

		tableID := event.TableID(*format)

		const clearTableMap = 0xFFFFFF
		if tableID == clearTableMap {
			tableMap := b.getTableMap(ctx)
			for k := range tableMap {
				delete(tableMap, k)
			}
			ctx.GetLogger().Tracef("Clearing table maps (special table ID 0xFFFFFF)")
			return nil
		}

		tableMap, err := event.TableMap(*format)
		if err != nil {
			return fmt.Errorf("failed to parse TABLE_MAP_EVENT: %w", err)
		}

		tableMapCache := b.getTableMap(ctx)
		tableMapCache[tableID] = tableMap

		ctx.GetLogger().Tracef("Mapped table ID %d to %s.%s", tableID, tableMap.Database, tableMap.Name)
		return nil

	case eventWriteRowsV0, eventWriteRowsV1, eventWriteRowsV2:
		format := b.getBinlogFormat(ctx)
		if format == nil {
			return fmt.Errorf("no binlog format available for WRITE_ROWS event")
		}
		return b.processWriteRowsEvent(ctx, event, format)

	case eventUpdateRowsV0, eventUpdateRowsV1, eventUpdateRowsV2:
		format := b.getBinlogFormat(ctx)
		if format == nil {
			return fmt.Errorf("no binlog format available for UPDATE_ROWS event")
		}
		return b.processUpdateRowsEvent(ctx, event, format)

	case eventDeleteRowsV0, eventDeleteRowsV1, eventDeleteRowsV2:
		format := b.getBinlogFormat(ctx)
		if format == nil {
			return fmt.Errorf("no binlog format available for DELETE_ROWS event")
		}
		return b.processDeleteRowsEvent(ctx, event, format)

	case eventQuery:
		// mariadb-binlog outputs QUERY_EVENT as regular SQL statements, not base64-encoded
		ctx.GetLogger().Tracef("Processing QUERY_EVENT (already handled by SQL parser)")
		return nil

	default:
		ctx.GetLogger().Tracef("Skipping unsupported event type: %d", eventType)
		return nil
	}
}

// processWriteRowsEvent decodes WRITE_ROWS events into row data and executes INSERT operations.
// The event contains a table ID, flags, column count, columns-used bitmap, per-row NULL bitmaps,
// and binary-encoded column values. If checksums are enabled, a CRC32 checksum is appended to the event.
func (b *Binlog) processWriteRowsEvent(ctx *sql.Context, ev mysql.BinlogEvent, format *mysql.BinlogFormat) error {
	if format.ChecksumAlgorithm != mysql.BinlogChecksumAlgOff && format.ChecksumAlgorithm != mysql.BinlogChecksumAlgUndef {
		var err error
		ev, _, err = ev.StripChecksum(*format)
		if err != nil {
			return fmt.Errorf("failed to strip checksum from WRITE_ROWS_EVENT: %w", err)
		}
	}

	tableID := ev.TableID(*format)
	tableMapCache := b.getTableMap(ctx)
	tableMap, ok := tableMapCache[tableID]
	if !ok {
		return fmt.Errorf("no table mapping found for table ID %d", tableID)
	}

	rows, err := ev.Rows(*format, tableMap)
	if err != nil {
		return fmt.Errorf("failed to parse WRITE_ROWS_EVENT: %w", err)
	}

	db, err := b.Catalog.Database(ctx, tableMap.Database)
	if err != nil {
		return fmt.Errorf("failed to get database %s: %w", tableMap.Database, err)
	}

	table, ok, err := db.GetTableInsensitive(ctx, tableMap.Name)
	if err != nil {
		return fmt.Errorf("failed to get table %s.%s: %w", tableMap.Database, tableMap.Name, err)
	}
	if !ok {
		return fmt.Errorf("table %s.%s not found", tableMap.Database, tableMap.Name)
	}

	schema := table.Schema()

	for _, row := range rows.Rows {
		sqlRow, err := b.parseRowData(ctx, tableMap, schema, row.NullColumns, row.Data)
		if err != nil {
			return fmt.Errorf("failed to parse row data: %w", err)
		}

		insertable, ok := table.(sql.InsertableTable)
		if !ok {
			return fmt.Errorf("table %s.%s is not insertable", tableMap.Database, tableMap.Name)
		}

		inserter := insertable.Inserter(ctx)
		err = inserter.Insert(ctx, sqlRow)
		if err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}

		err = inserter.Close(ctx)
		if err != nil {
			return fmt.Errorf("failed to close inserter: %w", err)
		}
	}

	ctx.GetLogger().Tracef("Inserted %d rows into %s.%s", len(rows.Rows), tableMap.Database, tableMap.Name)
	return nil
}

// processUpdateRowsEvent decodes UPDATE_ROWS events and executes UPDATE operations. The event
// contains before and after row images. The before image locates the row and the after image
// provides the new column values.
func (b *Binlog) processUpdateRowsEvent(ctx *sql.Context, ev mysql.BinlogEvent, format *mysql.BinlogFormat) error {
	if format.ChecksumAlgorithm != mysql.BinlogChecksumAlgOff && format.ChecksumAlgorithm != mysql.BinlogChecksumAlgUndef {
		var err error
		ev, _, err = ev.StripChecksum(*format)
		if err != nil {
			return fmt.Errorf("failed to strip checksum from UPDATE_ROWS_EVENT: %w", err)
		}
	}

	tableID := ev.TableID(*format)
	tableMapCache := b.getTableMap(ctx)
	tableMap, ok := tableMapCache[tableID]
	if !ok {
		return fmt.Errorf("no table mapping found for table ID %d", tableID)
	}

	rows, err := ev.Rows(*format, tableMap)
	if err != nil {
		return fmt.Errorf("failed to parse UPDATE_ROWS_EVENT: %w", err)
	}

	db, err := b.Catalog.Database(ctx, tableMap.Database)
	if err != nil {
		return fmt.Errorf("failed to get database %s: %w", tableMap.Database, err)
	}

	table, ok, err := db.GetTableInsensitive(ctx, tableMap.Name)
	if err != nil {
		return fmt.Errorf("failed to get table %s.%s: %w", tableMap.Database, tableMap.Name, err)
	}
	if !ok {
		return fmt.Errorf("table %s.%s not found", tableMap.Database, tableMap.Name)
	}

	schema := table.Schema()

	updatable, ok := table.(sql.UpdatableTable)
	if !ok {
		return fmt.Errorf("table %s.%s is not updatable", tableMap.Database, tableMap.Name)
	}

	updater := updatable.Updater(ctx)
	defer updater.Close(ctx)

	for _, row := range rows.Rows {
		oldRow, err := b.parseRowData(ctx, tableMap, schema, row.NullIdentifyColumns, row.Identify)
		if err != nil {
			return fmt.Errorf("failed to parse old row data: %w", err)
		}

		newRow, err := b.parseRowData(ctx, tableMap, schema, row.NullColumns, row.Data)
		if err != nil {
			return fmt.Errorf("failed to parse new row data: %w", err)
		}

		err = updater.Update(ctx, oldRow, newRow)
		if err != nil {
			return fmt.Errorf("failed to update row: %w", err)
		}
	}

	ctx.GetLogger().Tracef("Updated %d rows in %s.%s", len(rows.Rows), tableMap.Database, tableMap.Name)
	return nil
}

// processDeleteRowsEvent decodes DELETE_ROWS events and executes DELETE operations. The event
// contains row identification data but no after image since rows are being removed.
func (b *Binlog) processDeleteRowsEvent(ctx *sql.Context, ev mysql.BinlogEvent, format *mysql.BinlogFormat) error {
	if format.ChecksumAlgorithm != mysql.BinlogChecksumAlgOff && format.ChecksumAlgorithm != mysql.BinlogChecksumAlgUndef {
		var err error
		ev, _, err = ev.StripChecksum(*format)
		if err != nil {
			return fmt.Errorf("failed to strip checksum from DELETE_ROWS_EVENT: %w", err)
		}
	}

	tableID := ev.TableID(*format)
	tableMapCache := b.getTableMap(ctx)
	tableMap, ok := tableMapCache[tableID]
	if !ok {
		return fmt.Errorf("no table mapping found for table ID %d", tableID)
	}

	rows, err := ev.Rows(*format, tableMap)
	if err != nil {
		return fmt.Errorf("failed to parse DELETE_ROWS_EVENT: %w", err)
	}

	db, err := b.Catalog.Database(ctx, tableMap.Database)
	if err != nil {
		return fmt.Errorf("failed to get database %s: %w", tableMap.Database, err)
	}

	table, ok, err := db.GetTableInsensitive(ctx, tableMap.Name)
	if err != nil {
		return fmt.Errorf("failed to get table %s.%s: %w", tableMap.Database, tableMap.Name, err)
	}
	if !ok {
		return fmt.Errorf("table %s.%s not found", tableMap.Database, tableMap.Name)
	}

	schema := table.Schema()

	deletable, ok := table.(sql.DeletableTable)
	if !ok {
		return fmt.Errorf("table %s.%s is not deletable", tableMap.Database, tableMap.Name)
	}

	deleter := deletable.Deleter(ctx)
	defer deleter.Close(ctx)

	for _, row := range rows.Rows {
		sqlRow, err := b.parseRowData(ctx, tableMap, schema, row.NullIdentifyColumns, row.Identify)
		if err != nil {
			return fmt.Errorf("failed to parse row data: %w", err)
		}

		err = deleter.Delete(ctx, sqlRow)
		if err != nil {
			return fmt.Errorf("failed to delete row: %w", err)
		}
	}

	ctx.GetLogger().Tracef("Deleted %d rows from %s.%s", len(rows.Rows), tableMap.Database, tableMap.Name)
	return nil
}

// parseRowData decodes binary row data into a sql.Row. Each column is encoded according to its
// MySQL type using a format determined by the type code and metadata from the TABLE_MAP event.
// NULL values are indicated by a bitmap rather than stored in the data. Variable-length types
// like VARCHAR and BLOB include length prefixes before the data.
func (b *Binlog) parseRowData(ctx *sql.Context, tableMap *mysql.TableMap, schema sql.Schema, nullBitmap mysql.Bitmap, data []byte) (sql.Row, error) {
	row := make(sql.Row, len(schema))
	pos := 0

	for i, column := range schema {
		if nullBitmap.Bit(i) {
			row[i] = nil
			continue
		}

		value, length, err := mysql.CellValue(data, pos, tableMap.Types[i], tableMap.Metadata[i], vquery.Type_UINT64)
		if err != nil {
			return nil, fmt.Errorf("failed to decode cell value for column %d: %w", i, err)
		}
		pos += length

		convertedValue, err := b.convertValue(ctx, value, column)
		if err != nil {
			return nil, fmt.Errorf("failed to convert value for column %s: %w", column.Name, err)
		}
		row[i] = convertedValue
	}

	return row, nil
}

// convertValue converts a Vitess sqltypes.Value to a Go value compatible with the column's SQL type.
func (b *Binlog) convertValue(ctx *sql.Context, value sqltypes.Value, column *sql.Column) (interface{}, error) {
	if value.IsNull() {
		return nil, nil
	}

	converted, _, err := column.Type.Convert(ctx, value.ToString())
	if err != nil {
		return nil, err
	}

	return converted, nil
}

func (b *Binlog) String() string {
	return "BINLOG"
}

func (b *Binlog) Resolved() bool {
	return true
}

func (b *Binlog) Schema() sql.Schema {
	return nil
}

func (b *Binlog) Children() []sql.Node {
	return nil
}

func (b *Binlog) IsReadOnly() bool {
	return false
}

// WithChildren implements the Node interface.
func (b *Binlog) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(b, len(children), 0)
	}
	return b, nil
}
