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
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/test"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"
)

func TestBuildBinlog_InvalidBase64(t *testing.T) {
	builder := &BaseBuilder{}
	ctx := sql.NewEmptyContext()
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	catalog := test.NewCatalog(pro)

	binlogNode := plan.NewBinlog("invalid!@#$base64", catalog)

	_, err := builder.buildBinlog(ctx, binlogNode, nil)
	require.Error(t, err)
	require.True(t, sql.ErrBase64DecodeError.Is(err))
}

func TestBuildBinlog_EmptyString(t *testing.T) {
	builder := &BaseBuilder{}
	ctx := sql.NewEmptyContext()
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	catalog := test.NewCatalog(pro)

	binlogNode := plan.NewBinlog("", catalog)

	iter, err := builder.buildBinlog(ctx, binlogNode, nil)
	require.NoError(t, err)
	require.NotNil(t, iter)

	row, err := iter.Next(ctx)
	require.Equal(t, io.EOF, err)
	require.Nil(t, row)
}

func TestBuildBinlog_IncompleteEventHeader(t *testing.T) {
	builder := &BaseBuilder{}
	ctx := sql.NewEmptyContext()
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	catalog := test.NewCatalog(pro)

	// Create a buffer with less than eventHeaderSize (19 bytes)
	shortData := make([]byte, 10)
	encoded := base64.StdEncoding.EncodeToString(shortData)

	binlogNode := plan.NewBinlog(encoded, catalog)

	iter, err := builder.buildBinlog(ctx, binlogNode, nil)
	require.NoError(t, err)

	_, err = iter.Next(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "incomplete event header")
}

func TestBuildBinlog_IncompleteEvent(t *testing.T) {
	builder := &BaseBuilder{}
	ctx := sql.NewEmptyContext()
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	catalog := test.NewCatalog(pro)

	// Create event header with length larger than actual data
	eventData := make([]byte, eventHeaderSize)
	// Set event length to 1000 at offset 9
	binary.LittleEndian.PutUint32(eventData[eventLengthOffset:], 1000)

	encoded := base64.StdEncoding.EncodeToString(eventData)

	binlogNode := plan.NewBinlog(encoded, catalog)

	iter, err := builder.buildBinlog(ctx, binlogNode, nil)
	require.NoError(t, err)

	_, err = iter.Next(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "incomplete event")
	require.Contains(t, err.Error(), "exceeds buffer")
}

func TestBuildBinlog_MultilineBase64(t *testing.T) {
	builder := &BaseBuilder{}
	ctx := sql.NewEmptyContext()
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	catalog := test.NewCatalog(pro)

	eventData := make([]byte, eventHeaderSize)
	binary.LittleEndian.PutUint32(eventData[eventLengthOffset:], uint32(eventHeaderSize))
	eventData[eventTypeOffset] = eventWriteRowsV1

	part1 := base64.StdEncoding.EncodeToString(eventData[:10])
	part2 := base64.StdEncoding.EncodeToString(eventData[10:])
	multiline := part1 + "\n" + part2

	binlogNode := plan.NewBinlog(multiline, catalog)

	iter, err := builder.buildBinlog(ctx, binlogNode, nil)
	require.NoError(t, err)
	require.NotNil(t, iter)
}

func TestProcessEvent_UnsupportedEventType(t *testing.T) {
	ctx := sql.NewEmptyContext()
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	catalog := test.NewCatalog(pro)

	// Create event with unsupported type
	eventData := make([]byte, eventHeaderSize)
	binary.LittleEndian.PutUint32(eventData[eventLengthOffset:], uint32(eventHeaderSize))
	eventData[eventTypeOffset] = 4 // ROTATE_EVENT

	err := processEvent(ctx, catalog, 4, eventData)
	require.Error(t, err)
	require.True(t, sql.ErrOnlyFDAndRBREventsAllowedInBinlogStatement.Is(err))
}

func TestProcessEvent_QueryEvent(t *testing.T) {
	ctx := sql.NewEmptyContext()
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	catalog := test.NewCatalog(pro)

	binlogSess.format = nil
	binlogSessClearTableMaps()

	eventData := make([]byte, eventHeaderSize)
	binary.LittleEndian.PutUint32(eventData[eventLengthOffset:], uint32(eventHeaderSize))

	// In the future if QUERY_EVENT(2) support is added, this should fail without FORMAT_DESCRIPTION_EVENT.
	err := processEvent(ctx, catalog, 2, eventData)
	require.Error(t, err)
	require.True(t, sql.ErrOnlyFDAndRBREventsAllowedInBinlogStatement.Is(err))
}

func TestProcessEvent_NoFormatDescriptionEvent(t *testing.T) {
	ctx := sql.NewEmptyContext()
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	catalog := test.NewCatalog(pro)

	binlogSess.format = nil
	binlogSessClearTableMaps()

	eventData := make([]byte, eventHeaderSize+100)
	binary.LittleEndian.PutUint32(eventData[eventLengthOffset:], uint32(len(eventData)))

	err := processEvent(ctx, catalog, eventWriteRowsV2, eventData)
	require.Error(t, err)
	require.True(t, sql.ErrNoFormatDescriptionEventBeforeBinlogStatement.Is(err))
}

func TestBinlogSessClearTableMaps(t *testing.T) {
	binlogSess.tableMapByID[1] = nil
	binlogSess.tableMapByID[2] = nil
	require.Equal(t, 2, len(binlogSess.tableMapByID))

	binlogSessClearTableMaps()
	require.Equal(t, 0, len(binlogSess.tableMapByID))
}

func TestConvertValue_NullValue(t *testing.T) {
	ctx := sql.NewEmptyContext()
	col := &sql.Column{
		Name: "test",
		Type: types.Int32,
	}

	nullValue := sqltypes.NULL
	result, err := convertValue(ctx, nullValue, col)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestConvertValue_IntegerConversion(t *testing.T) {
	ctx := sql.NewEmptyContext()
	col := &sql.Column{
		Name: "test",
		Type: types.Int32,
	}

	value := sqltypes.NewInt32(123)
	result, err := convertValue(ctx, value, col)
	require.NoError(t, err)
	require.Equal(t, int32(123), result)
}

func TestConvertValue_StringConversion(t *testing.T) {
	ctx := sql.NewEmptyContext()
	col := &sql.Column{
		Name: "test",
		Type: types.Text,
	}

	value := sqltypes.NewVarChar("hello")
	result, err := convertValue(ctx, value, col)
	require.NoError(t, err)
	require.Equal(t, "hello", result)
}

func TestProcessEvent_TableMapEvent_NoTableMapsFound(t *testing.T) {
	ctx := sql.NewEmptyContext()
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	catalog := test.NewCatalog(pro)

	binlogSess.format = nil
	binlogSessClearTableMaps()

	eventData := make([]byte, eventHeaderSize+100)
	binary.LittleEndian.PutUint32(eventData[eventLengthOffset:], uint32(len(eventData)))

	err := processEvent(ctx, catalog, eventTableMap, eventData)
	require.Error(t, err)
	require.True(t, sql.ErrNoFormatDescriptionEventBeforeBinlogStatement.Is(err))
}

func TestProcessEvent_WriteRowsEvent_NoTableMapFound(t *testing.T) {
	ctx := sql.NewEmptyContext()
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	catalog := test.NewCatalog(pro)

	// Set up a minimal format but no table maps
	// HeaderSizes needs to be initialized with 256 entries (one per event type)
	headerSizes := make([]byte, 256)
	for i := range headerSizes {
		headerSizes[i] = 8 // Common post-header length for row events
	}
	binlogSess.format = &mysql.BinlogFormat{
		ChecksumAlgorithm: mysql.BinlogChecksumAlgOff,
		HeaderSizes:       headerSizes,
	}
	binlogSessClearTableMaps()

	// Create a minimal WRITE_ROWS event with table ID 1
	eventData := make([]byte, eventHeaderSize+100)
	binary.LittleEndian.PutUint32(eventData[eventLengthOffset:], uint32(len(eventData)))
	// Set table ID to 1 (post-header starts at eventHeaderSize)
	// Table ID is at offset 0 of post-header for row events (6 bytes, little endian)
	binary.LittleEndian.PutUint32(eventData[eventHeaderSize:], 1)

	err := processEvent(ctx, catalog, eventWriteRowsV2, eventData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no table mapping found for table ID")
}

func TestProcessEvent_UpdateRowsEvent_NoTableMapFound(t *testing.T) {
	ctx := sql.NewEmptyContext()
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	catalog := test.NewCatalog(pro)

	headerSizes := make([]byte, 256)
	for i := range headerSizes {
		headerSizes[i] = 8
	}
	binlogSess.format = &mysql.BinlogFormat{
		ChecksumAlgorithm: mysql.BinlogChecksumAlgOff,
		HeaderSizes:       headerSizes,
	}
	binlogSessClearTableMaps()

	// Create a minimal UPDATE_ROWS event with table ID 2
	eventData := make([]byte, eventHeaderSize+100)
	binary.LittleEndian.PutUint32(eventData[eventLengthOffset:], uint32(len(eventData)))
	binary.LittleEndian.PutUint32(eventData[eventHeaderSize:], 2)

	err := processEvent(ctx, catalog, eventUpdateRowsV2, eventData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no table mapping found for table ID")
}

func TestProcessEvent_DeleteRowsEvent_NoTableMapFound(t *testing.T) {
	ctx := sql.NewEmptyContext()
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	catalog := test.NewCatalog(pro)

	headerSizes := make([]byte, 256)
	for i := range headerSizes {
		headerSizes[i] = 8
	}
	binlogSess.format = &mysql.BinlogFormat{
		ChecksumAlgorithm: mysql.BinlogChecksumAlgOff,
		HeaderSizes:       headerSizes,
	}
	binlogSessClearTableMaps()

	// Create a minimal DELETE_ROWS event with table ID 3
	eventData := make([]byte, eventHeaderSize+100)
	binary.LittleEndian.PutUint32(eventData[eventLengthOffset:], uint32(len(eventData)))
	binary.LittleEndian.PutUint32(eventData[eventHeaderSize:], 3)

	err := processEvent(ctx, catalog, eventDeleteRowsV2, eventData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no table mapping found for table ID")
}

func TestTableMapByID_MultipleTableMaps(t *testing.T) {
	binlogSessClearTableMaps()

	tableMap1 := &mysql.TableMap{Database: "db1", Name: "table1"}
	tableMap2 := &mysql.TableMap{Database: "db2", Name: "table2"}
	tableMap3 := &mysql.TableMap{Database: "db3", Name: "table3"}

	binlogSess.tableMapByID[100] = tableMap1
	binlogSess.tableMapByID[200] = tableMap2
	binlogSess.tableMapByID[300] = tableMap3

	require.Equal(t, 3, len(binlogSess.tableMapByID))
	require.Equal(t, "table1", binlogSess.tableMapByID[100].Name)
	require.Equal(t, "table2", binlogSess.tableMapByID[200].Name)
	require.Equal(t, "table3", binlogSess.tableMapByID[300].Name)
}

func TestTableMapByID_OverwriteExistingMap(t *testing.T) {
	binlogSessClearTableMaps()

	tableMap1 := &mysql.TableMap{Database: "db1", Name: "table1"}
	tableMap2 := &mysql.TableMap{Database: "db2", Name: "table2"}

	binlogSess.tableMapByID[100] = tableMap1
	require.Equal(t, "table1", binlogSess.tableMapByID[100].Name)

	binlogSess.tableMapByID[100] = tableMap2
	require.Equal(t, "table2", binlogSess.tableMapByID[100].Name)
	require.Equal(t, 1, len(binlogSess.tableMapByID))
}

func TestTableMapByID_RetrievalAfterClear(t *testing.T) {
	binlogSessClearTableMaps()

	tableMap := &mysql.TableMap{Database: "db1", Name: "table1"}
	binlogSess.tableMapByID[1] = tableMap

	retrieved, ok := binlogSess.tableMapByID[1]
	require.True(t, ok)
	require.NotNil(t, retrieved)

	binlogSessClearTableMaps()
	retrieved, ok = binlogSess.tableMapByID[1]
	require.False(t, ok)
	require.Nil(t, retrieved)
}

func TestTableMapByID_LargeTableIDs(t *testing.T) {
	binlogSessClearTableMaps()

	tableMap := &mysql.TableMap{Database: "db1", Name: "table1"}
	largeID := uint64(0xFFFFFE)

	binlogSess.tableMapByID[largeID] = tableMap
	retrieved, ok := binlogSess.tableMapByID[largeID]
	require.True(t, ok)
	require.Equal(t, "table1", retrieved.Name)
}

func TestProcessEvent_TransactionBoundaryEvents(t *testing.T) {
	ctx := sql.NewEmptyContext()
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	catalog := test.NewCatalog(pro)

	eventData := make([]byte, eventHeaderSize)
	binary.LittleEndian.PutUint32(eventData[eventLengthOffset:], uint32(eventHeaderSize))

	err := processEvent(ctx, catalog, eventXID, eventData)
	require.NoError(t, err)

	err = processEvent(ctx, catalog, eventGTIDLogEvent, eventData)
	require.NoError(t, err)

	err = processEvent(ctx, catalog, eventAnonymousGTIDLogEvent, eventData)
	require.NoError(t, err)

	err = processEvent(ctx, catalog, eventPreviousGTIDsLogEvent, eventData)
	require.NoError(t, err)

	err = processEvent(ctx, catalog, eventGTIDMariaDB, eventData)
	require.NoError(t, err)

	err = processEvent(ctx, catalog, eventGTIDList, eventData)
	require.NoError(t, err)

	err = processEvent(ctx, catalog, eventAnnotateRows, eventData)
	require.NoError(t, err)
}
