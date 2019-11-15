package sql

type BaseType uint16

const (
	BaseType_NULL BaseType = 0
	BaseType_BIGINT BaseType = 20
	BaseType_BINARY BaseType = 40
	BaseType_BIT BaseType = 60
	BaseType_BLOB BaseType = 80
	BaseType_BOOLEAN BaseType = 100
	BaseType_CHAR BaseType = 120
	BaseType_DATE BaseType = 140
	BaseType_DATETIME BaseType = 160
	BaseType_DECIMAL BaseType = 180
	BaseType_DOUBLE BaseType = 200
	BaseType_ENUM BaseType = 220
	BaseType_FLOAT BaseType = 240
	BaseType_GEOMETRY BaseType = 260
	BaseType_GEOMETRYCOLLECTION BaseType = 280
	BaseType_INT BaseType = 300
	BaseType_JSON BaseType = 320
	BaseType_LINESTRING BaseType = 340
	BaseType_LONGBLOB BaseType = 360
	BaseType_LONGTEXT BaseType = 380
	BaseType_MEDIUMBLOB BaseType = 400
	BaseType_MEDIUMINT BaseType = 420
	BaseType_MEDIUMTEXT BaseType = 440
	BaseType_MULTILINESTRING BaseType = 460
	BaseType_MULTIPOINT BaseType = 480
	BaseType_MULTIPOLYGON BaseType = 500
	BaseType_POINT BaseType = 520
	BaseType_POLYGON BaseType = 540
	BaseType_SET BaseType = 560
	BaseType_SMALLINT BaseType = 580
	BaseType_TEXT BaseType = 600
	BaseType_TIME BaseType = 620
	BaseType_TIMESTAMP BaseType = 640
	BaseType_TINYBLOB BaseType = 660
	BaseType_TINYINT BaseType = 680
	BaseType_TINYTEXT BaseType = 700
	BaseType_VARBINARY BaseType = 720
	BaseType_VARCHAR BaseType = 740
	BaseType_YEAR BaseType = 760

	BaseType_Internal_EXPRESSION BaseType = 33000
)

func (bt BaseType) String() string {
	return baseTypeToString[bt]
}

var baseTypeToString = map[BaseType]string{
	BaseType_NULL: "",
	BaseType_BIGINT: "BIGINT",
	BaseType_BINARY: "BINARY",
	BaseType_BIT: "BIT",
	BaseType_BLOB: "BLOB",
	BaseType_BOOLEAN: "BOOLEAN",
	BaseType_CHAR: "CHAR",
	BaseType_DATE: "DATE",
	BaseType_DATETIME: "DATETIME",
	BaseType_DECIMAL: "DECIMAL",
	BaseType_DOUBLE: "DOUBLE",
	BaseType_ENUM: "ENUM",
	BaseType_FLOAT: "FLOAT",
	BaseType_GEOMETRY: "GEOMETRY",
	BaseType_GEOMETRYCOLLECTION: "GEOMETRYCOLLECTION",
	BaseType_INT: "INT",
	BaseType_JSON: "JSON",
	BaseType_LINESTRING: "LINESTRING",
	BaseType_LONGBLOB: "LONGBLOB",
	BaseType_LONGTEXT: "LONGTEXT",
	BaseType_MEDIUMBLOB: "MEDIUMBLOB",
	BaseType_MEDIUMINT: "MEDIUMINT",
	BaseType_MEDIUMTEXT: "MEDIUMTEXT",
	BaseType_MULTILINESTRING: "MULTILINESTRING",
	BaseType_MULTIPOINT: "MULTIPOINT",
	BaseType_MULTIPOLYGON: "MULTIPOLYGON",
	BaseType_POINT: "POINT",
	BaseType_POLYGON: "POLYGON",
	BaseType_SET: "SET",
	BaseType_SMALLINT: "SMALLINT",
	BaseType_TEXT: "TEXT",
	BaseType_TIME: "TIME",
	BaseType_TIMESTAMP: "TIMESTAMP",
	BaseType_TINYBLOB: "TINYBLOB",
	BaseType_TINYINT: "TINYINT",
	BaseType_TINYTEXT: "TINYTEXT",
	BaseType_VARBINARY: "VARBINARY",
	BaseType_VARCHAR: "VARCHAR",
	BaseType_YEAR: "YEAR",
}