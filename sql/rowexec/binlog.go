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

package rowexec

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// See https://github.com/mysql/mysql-server/blob/trunk/libs/mysql/binlog/event/binlog_event.h
// See https://github.com/MariaDB/server/blob/mariadb-11.4.8/sql/log_event.h#L608-L671
const (
	eventFormatDescription = 15
	eventXID               = 16
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

	// MySQL-specific
	eventGTIDLogEvent          = 33
	eventAnonymousGTIDLogEvent = 34
	eventPreviousGTIDsLogEvent = 35

	// MariaDB-specific
	eventAnnotateRows = 160
	eventGTIDMariaDB  = 162
	eventGTIDList     = 163
)

const (
	eventTypeOffset   = 4
	eventLengthOffset = 9
	eventHeaderSize   = 19
)

// clearTableMapID is a special table ID (0xFFFFFF) that signals all table mappings should be cleared.
const clearTableMapID = 0xFFFFFF

// binlogSess holds state that persists across multiple BINLOG statements. The format field stores binlog
// format metadata from FORMAT_DESCRIPTION_EVENT. The tableMapByID field maps table IDs to table metadata from
// TABLE_MAP events. Table mappings are cleared when processing a new binlog file (detected by FORMAT_DESCRIPTION_EVENT)
// or when receiving the special table ID 0xFFFFFF, because table IDs are only unique within a single binlog file and
// can be reused across files.
//
// TODO: MariaDB stores this state per-connection at thd->rgi_fake->m_table_map. This currently uses a single global
// state shared by all connections; sufficient for now. This can be refactored to use a cache similar to
// PreparedDataCache that tracks state per connection ID with a map[uint32]*binlogSess protected by sync.RWMutex, and
// add DeleteSessionData(connID) cleanup via Engine.CloseSession.
// See https://github.com/MariaDB/server/blob/mariadb-11.4.8/sql/sql_binlog.cc#L270-L271
// See https://github.com/MariaDB/server/blob/mariadb-11.4.8/sql/rpl_rli.h#L811
var binlogSess = &struct {
	format       *mysql.BinlogFormat
	tableMapByID map[uint64]*mysql.TableMap
}{
	tableMapByID: make(map[uint64]*mysql.TableMap),
}

// binlogSessClearTableMaps removes all table mappings from the session cache.
func binlogSessClearTableMaps() {
	for k := range binlogSess.tableMapByID {
		delete(binlogSess.tableMapByID, k)
	}
}

// buildBinlog decodes base64 binlog events and applies them directly, bypassing the normal SQL execution path.
//
// MariaDB does this by creating a fake replication context and calling save_restore_context_apply_event(), which
// invokes ha_write_row(). Triggers are skipped because they're only invoked in the normal INSERT code path,
// not in ha_write_row().
// See https://github.com/MariaDB/server/blob/mariadb-11.4.8/sql/sql_binlog.cc#L267-L428
func (b *BaseBuilder) buildBinlog(ctx *sql.Context, n *plan.Binlog, row sql.Row) (sql.RowIter, error) {
	if n.Base64Str == "" {
		return nil, sql.ErrSyntaxError.New("BINLOG")
	}

	var decoded []byte

	lines := strings.Split(n.Base64Str, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		block, err := base64.StdEncoding.DecodeString(line)
		if err != nil {
			return nil, sql.ErrBase64DecodeError.New()
		}

		decoded = append(decoded, block...)
	}

	return &binlogIter{
		catalog: n.Catalog,
		decoded: decoded,
		offset:  0,
	}, nil
}

// binlogIter iterates through decoded binlog events and returns a single OkResult row after processing all events.
type binlogIter struct {
	catalog sql.Catalog
	decoded []byte
	offset  int
}

var _ sql.RowIter = (*binlogIter)(nil)

// Next processes binlog events recursively and returns a single OkResult row after all events are processed.
func (bi *binlogIter) Next(ctx *sql.Context) (sql.Row, error) {
	if bi.offset >= len(bi.decoded) {
		// All events processed, return OkResult once, then EOF
		if bi.offset == len(bi.decoded) {
			bi.offset++ // Mark OkResult as returned
			return sql.Row{types.OkResult{}}, nil
		}
		return nil, io.EOF
	}

	if bi.offset+eventHeaderSize > len(bi.decoded) {
		return nil, fmt.Errorf("incomplete event header at offset %d", bi.offset)
	}

	eventLength := binary.LittleEndian.Uint32(bi.decoded[bi.offset+eventLengthOffset : bi.offset+eventLengthOffset+4])

	if bi.offset+int(eventLength) > len(bi.decoded) {
		return nil, fmt.Errorf("incomplete event at offset %d: event length %d exceeds buffer", bi.offset, eventLength)
	}

	eventType := bi.decoded[bi.offset+eventTypeOffset]
	eventData := bi.decoded[bi.offset : bi.offset+int(eventLength)]

	if err := processEvent(ctx, bi.catalog, eventType, eventData); err != nil {
		return nil, err
	}

	bi.offset += int(eventLength)

	// Recurse to process next event immediately to not yield rows between events
	return bi.Next(ctx)
}

// Close implements sql.RowIter.
func (bi *binlogIter) Close(ctx *sql.Context) error {
	return nil
}

// processEvent dispatches a binlog event to its handler based on event type.
func processEvent(ctx *sql.Context, catalog sql.Catalog, eventType byte, eventData []byte) error {
	// MariaDB format is backward compatible with MySQL events
	event := mysql.NewMariadbBinlogEvent(eventData)

	switch eventType {
	case eventFormatDescription:
		parsedFormat, err := event.Format()
		if err != nil {
			return fmt.Errorf("failed to parse FORMAT_DESCRIPTION_EVENT: %w", err)
		}
		binlogSess.format = &parsedFormat

		binlogSessClearTableMaps()

		return nil

	case eventTableMap:
		format := binlogSess.format
		if format == nil {
			return sql.ErrNoFormatDescriptionEventBeforeBinlogStatement.New("TABLE_MAP_EVENT")
		}

		if format.ChecksumAlgorithm != mysql.BinlogChecksumAlgOff && format.ChecksumAlgorithm != mysql.BinlogChecksumAlgUndef {
			var err error
			event, _, err = event.StripChecksum(*format)
			if err != nil {
				return fmt.Errorf("failed to strip checksum from TABLE_MAP_EVENT: %w", err)
			}
		}

		tableID := event.TableID(*format)

		if tableID == clearTableMapID {
			binlogSessClearTableMaps()
			return nil
		}

		tableMap, err := event.TableMap(*format)
		if err != nil {
			return fmt.Errorf("failed to parse TABLE_MAP_EVENT: %w", err)
		}

		binlogSess.tableMapByID[tableID] = tableMap

		return nil

	case eventWriteRowsV0, eventWriteRowsV1, eventWriteRowsV2:
		format := binlogSess.format
		if format == nil {
			return sql.ErrNoFormatDescriptionEventBeforeBinlogStatement.New("WRITE_ROWS_EVENT")
		}
		return processEventWriteRows(ctx, catalog, event, format)

	case eventUpdateRowsV0, eventUpdateRowsV1, eventUpdateRowsV2:
		format := binlogSess.format
		if format == nil {
			return sql.ErrNoFormatDescriptionEventBeforeBinlogStatement.New("UPDATE_ROWS_EVENT")
		}
		return processEventUpdateRows(ctx, catalog, event, format)

	case eventDeleteRowsV0, eventDeleteRowsV1, eventDeleteRowsV2:
		format := binlogSess.format
		if format == nil {
			return sql.ErrNoFormatDescriptionEventBeforeBinlogStatement.New("DELETE_ROWS_EVENT")
		}
		return processEventDeleteRows(ctx, catalog, event, format)

	case eventXID, eventGTIDLogEvent, eventAnonymousGTIDLogEvent, eventPreviousGTIDsLogEvent, eventGTIDMariaDB, eventGTIDList, eventAnnotateRows:
		// Transaction boundary and metadata events can be safely ignored for BINLOG statement replay. Since each BINLOG
		// statement is auto-committed and we're not implementing full replication, these transaction boundaries and
		// metadata are no-ops.
		return nil

	default:
		return sql.ErrOnlyFDAndRBREventsAllowedInBinlogStatement.New(fmt.Sprintf("event type %d", eventType))
	}
}

// processEventWriteRows decodes WRITE_ROWS events into row data and executes INSERT operations.
func processEventWriteRows(ctx *sql.Context, catalog sql.Catalog, ev mysql.BinlogEvent, format *mysql.BinlogFormat) error {
	if format.ChecksumAlgorithm != mysql.BinlogChecksumAlgOff && format.ChecksumAlgorithm != mysql.BinlogChecksumAlgUndef {
		var err error
		ev, _, err = ev.StripChecksum(*format)
		if err != nil {
			return fmt.Errorf("failed to strip checksum from WRITE_ROWS_EVENT: %w", err)
		}
	}

	tableID := ev.TableID(*format)
	tableMapEv, ok := binlogSess.tableMapByID[tableID]
	if !ok {
		return fmt.Errorf("no table mapping found for table ID %d", tableID)
	}

	rows, err := ev.Rows(*format, tableMapEv)
	if err != nil {
		return fmt.Errorf("failed to parse WRITE_ROWS_EVENT: %w", err)
	}

	table, _, err := catalog.Table(ctx, tableMapEv.Database, tableMapEv.Name)
	if err != nil {
		return err
	}

	schema := table.Schema()

	for _, row := range rows.Rows {
		sqlRow, err := parseRowData(ctx, tableMapEv, schema, row.NullColumns, row.Data)
		if err != nil {
			return err
		}

		insertable, ok := table.(sql.InsertableTable)
		if !ok {
			return fmt.Errorf("table %s.%s is not insertable", tableMapEv.Database, tableMapEv.Name)
		}

		inserter := insertable.Inserter(ctx)
		err = inserter.Insert(ctx, sqlRow)
		if err != nil {
			return err
		}

		err = inserter.Close(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// processEventUpdateRows decodes UPDATE_ROWS events and executes UPDATE operations. The event contains before and
// after row images. The before image locates the row and the after image provides the new column values.
func processEventUpdateRows(ctx *sql.Context, catalog sql.Catalog, ev mysql.BinlogEvent, format *mysql.BinlogFormat) error {
	if format.ChecksumAlgorithm != mysql.BinlogChecksumAlgOff && format.ChecksumAlgorithm != mysql.BinlogChecksumAlgUndef {
		var err error
		ev, _, err = ev.StripChecksum(*format)
		if err != nil {
			return fmt.Errorf("failed to strip checksum from UPDATE_ROWS_EVENT: %w", err)
		}
	}

	tableID := ev.TableID(*format)
	tableMapEv, ok := binlogSess.tableMapByID[tableID]
	if !ok {
		return fmt.Errorf("no table mapping found for table ID %d", tableID)
	}

	rows, err := ev.Rows(*format, tableMapEv)
	if err != nil {
		return fmt.Errorf("failed to parse UPDATE_ROWS_EVENT: %w", err)
	}

	table, _, err := catalog.Table(ctx, tableMapEv.Database, tableMapEv.Name)
	if err != nil {
		return err
	}

	schema := table.Schema()

	updatable, ok := table.(sql.UpdatableTable)
	if !ok {
		return fmt.Errorf("table %s.%s is not updatable", tableMapEv.Database, tableMapEv.Name)
	}

	updater := updatable.Updater(ctx)
	defer updater.Close(ctx)

	for _, row := range rows.Rows {
		oldRow, err := parseRowData(ctx, tableMapEv, schema, row.NullIdentifyColumns, row.Identify)
		if err != nil {
			return err
		}

		newRow, err := parseRowData(ctx, tableMapEv, schema, row.NullColumns, row.Data)
		if err != nil {
			return err
		}

		err = updater.Update(ctx, oldRow, newRow)
		if err != nil {
			return err
		}
	}

	return nil
}

// processEventDeleteRows decodes DELETE_ROWS events and executes DELETE operations. The event contains row
// identification data but no after image since rows are being removed.
func processEventDeleteRows(ctx *sql.Context, catalog sql.Catalog, ev mysql.BinlogEvent, format *mysql.BinlogFormat) error {
	if format.ChecksumAlgorithm != mysql.BinlogChecksumAlgOff && format.ChecksumAlgorithm != mysql.BinlogChecksumAlgUndef {
		var err error
		ev, _, err = ev.StripChecksum(*format)
		if err != nil {
			return fmt.Errorf("failed to strip checksum from DELETE_ROWS_EVENT: %w", err)
		}
	}

	tableID := ev.TableID(*format)
	tableMapEv, ok := binlogSess.tableMapByID[tableID]
	if !ok {
		return fmt.Errorf("no table mapping found for table ID %d", tableID)
	}

	rows, err := ev.Rows(*format, tableMapEv)
	if err != nil {
		return fmt.Errorf("failed to parse DELETE_ROWS_EVENT: %w", err)
	}

	table, _, err := catalog.Table(ctx, tableMapEv.Database, tableMapEv.Name)
	if err != nil {
		return err
	}

	schema := table.Schema()

	deletable, ok := table.(sql.DeletableTable)
	if !ok {
		return fmt.Errorf("table %s.%s is not deletable", tableMapEv.Database, tableMapEv.Name)
	}

	deleter := deletable.Deleter(ctx)
	defer deleter.Close(ctx)

	for _, row := range rows.Rows {
		sqlRow, err := parseRowData(ctx, tableMapEv, schema, row.NullIdentifyColumns, row.Identify)
		if err != nil {
			return err
		}

		err = deleter.Delete(ctx, sqlRow)
		if err != nil {
			return err
		}
	}

	return nil
}

// parseRowData decodes binary row data into a sql.Row. Each column is encoded according to its MySQL type using a
// format determined by the type code and metadata from the TABLE_MAP event. NULL values are indicated by a bitmap
// rather than stored in the data. Variable-length types like VARCHAR and BLOB include length prefixes before the data.
func parseRowData(ctx *sql.Context, tableMap *mysql.TableMap, schema sql.Schema, nullBitmap mysql.Bitmap, data []byte) (sql.Row, error) {
	row := make(sql.Row, len(schema))
	pos := 0

	for i, column := range schema {
		if nullBitmap.Bit(i) {
			row[i] = nil
			continue
		}

		value, length, err := mysql.CellValue(data, pos, tableMap.Types[i], tableMap.Metadata[i], query.Type_UINT64)
		if err != nil {
			return nil, fmt.Errorf("failed to decode cell value for column %d: %w", i, err)
		}
		pos += length

		convertedValue, err := convertValue(ctx, value, column)
		if err != nil {
			return nil, err
		}
		row[i] = convertedValue
	}

	return row, nil
}

// convertValue converts a Vitess sqltypes.Value to a Go value compatible with the column's SQL type.
func convertValue(ctx *sql.Context, value sqltypes.Value, column *sql.Column) (interface{}, error) {
	if value.IsNull() {
		return nil, nil
	}

	converted, _, err := column.Type.Convert(ctx, value.ToString())
	if err != nil {
		return nil, err
	}

	return converted, nil
}
