package sql

const DualTableName = "dual"

var DualTableSchema = NewPrimaryKeySchema(Schema{
	{Name: "dummy", Source: DualTableName, Type: LongText, Nullable: false},
})

// IsDualTable returns whether the given table is the "dual" table.
func IsDualTable(t Table) bool {
	if t == nil {
		return false
	}
	return t.Name() == DualTableName && t.Schema().Equals(DualTableSchema.Schema)
}
