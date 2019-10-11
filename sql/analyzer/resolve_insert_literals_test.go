package analyzer

import (
	"testing"

	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
)

// Common data for most of the tests below
var (
	schema = sql.Schema{
		{Name: "i8", Type: sql.Int8, Source: "table"},
		{Name: "i16", Type: sql.Int16, Source: "table"},
		{Name: "i32", Type: sql.Int32, Source: "table"},
		{Name: "i64", Type: sql.Int64, Source: "table"},
		{Name: "ui8", Type: sql.Uint8, Source: "table"},
		{Name: "ui16", Type: sql.Uint16, Source: "table"},
		{Name: "ui32", Type: sql.Uint32, Source: "table"},
		{Name: "ui64", Type: sql.Uint64, Source: "table"},
	}

	orderedColumns = []string{"i8", "i16", "i32", "i64", "ui8", "ui16", "ui32", "ui64"}

	inputValues = [][]sql.Expression{{
		expression.NewLiteral(int8(1), sql.Int8),
		expression.NewLiteral(int8(1), sql.Int8),
		expression.NewLiteral(int8(1), sql.Int8),
		expression.NewLiteral(int8(1), sql.Int8),
		expression.NewLiteral(int8(1), sql.Uint8),
		expression.NewLiteral(int8(1), sql.Uint8),
		expression.NewLiteral(int8(1), sql.Uint8),
		expression.NewLiteral(int8(1), sql.Uint8),
	}}
)

// Test the correct conversion of integer literals in INSERT nodes when no
// columns are explicitely specified by the plan
func TestInsertLiteralsWithoutColumns(t *testing.T) {
	require := require.New(t)

	table := memory.NewTable("table", schema)

	// An INSERT node with an empty columns field: the expected columns should be
	// the ones in the schema
	node := plan.NewInsertInto(
		plan.NewResolvedTable(table),
		plan.NewValues(inputValues),
		false,
		[]string{},
	)

	rule := getRule("resolve_insert_literals")
	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
	require.NoError(err)

	// The expected result should have the integers converted to the types
	// specified by the schema, as well as the columns field populated with the
	// schema columns in order
	expected := plan.NewInsertInto(
		plan.NewResolvedTable(table),
		plan.NewValues([][]sql.Expression{{
			expression.NewLiteral(int8(1), sql.Int8),
			expression.NewLiteral(int16(1), sql.Int16),
			expression.NewLiteral(int32(1), sql.Int32),
			expression.NewLiteral(int64(1), sql.Int64),
			expression.NewLiteral(uint8(1), sql.Uint8),
			expression.NewLiteral(uint16(1), sql.Uint16),
			expression.NewLiteral(uint32(1), sql.Uint32),
			expression.NewLiteral(uint64(1), sql.Uint64),
		}}),
		false,
		orderedColumns,
	)

	require.Equal(expected, result)
}

// Test the correct conversion of integer literals in INSERT nodes when the
// node has a explicit order of columns, different than the one in the schema
func TestInsertLiteralsWithColumns(t *testing.T) {
	require := require.New(t)

	table := memory.NewTable("table", schema)

	// First unsigned, then signed
	unorderedColumns := []string{"ui8", "ui16", "ui32", "ui64", "i8", "i16", "i32", "i64"}

	// An INSERT node with an explicit columns field, unordered with respect to
	// the schema
	node := plan.NewInsertInto(
		plan.NewResolvedTable(table),
		plan.NewValues(inputValues),
		false,
		unorderedColumns,
	)

	rule := getRule("resolve_insert_literals")
	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
	require.NoError(err)

	// The expected result should have the integers converted to the types
	// specified by the schema, in the order specified by the columns field of
	// the INSERT node
	expected := plan.NewInsertInto(
		plan.NewResolvedTable(table),
		plan.NewValues([][]sql.Expression{{
			expression.NewLiteral(uint8(1), sql.Uint8),
			expression.NewLiteral(uint16(1), sql.Uint16),
			expression.NewLiteral(uint32(1), sql.Uint32),
			expression.NewLiteral(uint64(1), sql.Uint64),
			expression.NewLiteral(int8(1), sql.Int8),
			expression.NewLiteral(int16(1), sql.Int16),
			expression.NewLiteral(int32(1), sql.Int32),
			expression.NewLiteral(int64(1), sql.Int64),
		}}),
		false,
		unorderedColumns,
	)

	require.Equal(expected, result)
}

// Test that non-integer literals are unchanged after applying the conversion
// of integers in INSERT nodes
func TestInsertLiteralsUnchanged(t *testing.T) {
	require := require.New(t)

	table := memory.NewTable("table", sql.Schema{
		{Name: "f32", Type: sql.Float32, Source: "typestable", Nullable: true},
		{Name: "f64", Type: sql.Float64, Source: "typestable", Nullable: true},
		{Name: "time", Type: sql.Timestamp, Source: "typestable", Nullable: true},
		{Name: "date", Type: sql.Date, Source: "typestable", Nullable: true},
		{Name: "text", Type: sql.Text, Source: "typestable", Nullable: true},
		{Name: "bool", Type: sql.Boolean, Source: "typestable", Nullable: true},
		{Name: "json", Type: sql.JSON, Source: "typestable", Nullable: true},
		{Name: "blob", Type: sql.Blob, Source: "typestable", Nullable: true},
	})

	node := plan.NewInsertInto(
		plan.NewResolvedTable(table),
		plan.NewValues([][]sql.Expression{{
			expression.NewLiteral(float64(1.0), sql.Float32),
			expression.NewLiteral(float64(5.0), sql.Float64),
			expression.NewLiteral("1234-05-06 07:08:09", sql.Timestamp),
			expression.NewLiteral("1234-05-06", sql.Date),
			expression.NewLiteral("there be dragons", sql.Text),
			expression.NewLiteral(false, sql.Boolean),
			expression.NewLiteral(`{"key":"value"}`, sql.JSON),
			expression.NewLiteral("blipblop", sql.Blob),
		}}),
		false,
		[]string{"f32", "f64", "time", "date", "text", "bool", "json", "blob"},
	)

	rule := getRule("resolve_insert_literals")
	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
	require.NoError(err)

	// The original node should be unchanged, as there are no integers
	require.Equal(node, result)
}
