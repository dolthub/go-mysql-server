package sql

import "reflect"

// Column is the definition of a table column.
// As SQL:2016 puts it:
//   A column is a named component of a table. It has a data type, a default,
//   and a nullability characteristic.
type Column struct {
	// Name is the name of the column.
	Name string
	// Type is the data type of the column.
	Type Type
	// Default contains the default value of the column or nil if it was not explicitly defined. A nil instance is valid, thus calls do not error.
	Default *ColumnDefaultValue
	// Nullable is true if the column can contain NULL values, or false
	// otherwise.
	Nullable bool
	// Source is the name of the table this column came from.
	Source string
	// PrimaryKey is true if the column is part of the primary key for its table.
	PrimaryKey bool
	// Comment contains the string comment for this column
	Comment string
}

// Check ensures the value is correct for this column.
func (c *Column) Check(v interface{}) bool {
	if v == nil {
		return c.Nullable
	}

	_, err := c.Type.Convert(v)
	return err == nil
}

// Equals checks whether two columns are equal.
func (c *Column) Equals(c2 *Column) bool {
	return c.Name == c2.Name &&
		c.Source == c2.Source &&
		c.Nullable == c2.Nullable &&
		reflect.DeepEqual(c.Default, c2.Default) &&
		reflect.DeepEqual(c.Type, c2.Type)
}
