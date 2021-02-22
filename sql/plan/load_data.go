package plan

import "github.com/dolthub/go-mysql-server/sql"

type LoadData struct {
	Local bool
	File string
	Destination sql.Node
	ColumnNames []string
}

func (l LoadData) Resolved() bool {
	return l.Destination.Resolved()
}

func (l LoadData) String() string {
	return "Load data yooyoyoy"
}

func (l LoadData) Schema() sql.Schema {
	return l.Destination.Schema()
}

func (l LoadData) Children() []sql.Node {
	return []sql.Node{l.Destination}
}

func (l LoadData) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// Get Files as an InsertRows
	// Pass to InsertIter

	// Need to handle a bunch of edge cases and error handling
	retulrn nil, nil
}

func (l LoadData) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}

	l.Destination = children[0]
	return l, nil
}

func NewLoadData(local bool, file string, destination sql.Node, cols []string) *LoadData {
	return &LoadData{
		Local: local,
		File: file,
		Destination: destination,
		ColumnNames: cols,
	}
}