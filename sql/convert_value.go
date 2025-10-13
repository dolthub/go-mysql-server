package sql

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql/values"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// ConvertToValue converts the interface to a sql value.
func ConvertToValue(v interface{}) (sqltypes.Value, error) {
	switch v := v.(type) {
	case nil:
		return sqltypes.MakeTrusted(query.Type_NULL_TYPE, nil), nil
	case int:
		return sqltypes.MakeTrusted(query.Type_INT64, values.WriteInt64(make([]byte, values.Int64Size), int64(v))), nil
	case int8:
		return sqltypes.MakeTrusted(query.Type_INT8, values.WriteInt8(make([]byte, values.Int8Size), v)), nil
	case int16:
		return sqltypes.MakeTrusted(query.Type_INT16, values.WriteInt16(make([]byte, values.Int16Size), v)), nil
	case int32:
		return sqltypes.MakeTrusted(query.Type_INT32, values.WriteInt32(make([]byte, values.Int32Size), v)), nil
	case int64:
		return sqltypes.MakeTrusted(query.Type_INT64, values.WriteInt64(make([]byte, values.Int64Size), v)), nil
	case uint:
		return sqltypes.MakeTrusted(query.Type_UINT64, values.WriteUint64(make([]byte, values.Uint64Size), uint64(v))), nil
	case uint8:
		return sqltypes.MakeTrusted(query.Type_UINT8, values.WriteUint8(make([]byte, values.Uint8Size), v)), nil
	case uint16:
		return sqltypes.MakeTrusted(query.Type_UINT16, values.WriteUint16(make([]byte, values.Uint16Size), v)), nil
	case uint32:
		return sqltypes.MakeTrusted(query.Type_UINT32, values.WriteUint32(make([]byte, values.Uint32Size), v)), nil
	case uint64:
		return sqltypes.MakeTrusted(query.Type_UINT64, values.WriteUint64(make([]byte, values.Uint64Size), v)), nil
	case float32:
		return sqltypes.MakeTrusted(query.Type_FLOAT32, values.WriteFloat32(make([]byte, values.Float32Size), v)), nil
	case float64:
		return sqltypes.MakeTrusted(query.Type_FLOAT64, values.WriteFloat64(make([]byte, values.Float64Size), v)), nil
	case string:
		return sqltypes.MakeTrusted(query.Type_VARCHAR, values.WriteString(make([]byte, len(v)), v, values.ByteOrderCollation)), nil
	case []byte:
		return sqltypes.MakeTrusted(query.Type_BLOB, values.WriteBytes(make([]byte, len(v)), v, values.ByteOrderCollation)), nil
	default:
		return sqltypes.Value{}, fmt.Errorf("type %T not implemented", v)
	}
}
