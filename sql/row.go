package sql

type Row interface {
	Fields() []interface{}
}

type RowIter interface {
	Next() (Row, error)
}

type MemoryRow []interface{}

func NewMemoryRow(fields ...interface{}) MemoryRow {
	return MemoryRow(fields)
}

func (r MemoryRow) Fields() []interface{} {
	return []interface{}(r)
}
