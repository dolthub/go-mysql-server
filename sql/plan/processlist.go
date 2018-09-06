package plan

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

type process struct {
	id      int64
	user    string
	host    string
	db      string
	command string
	time    int64
	state   string
	info    string
}

func (p process) toRow() sql.Row {
	return sql.NewRow(
		p.id,
		p.user,
		p.host,
		p.db,
		p.command,
		p.time,
		p.state,
		p.info,
	)
}

var processListSchema = sql.Schema{
	{Name: "Id", Type: sql.Int64},
	{Name: "User", Type: sql.Text},
	{Name: "Host", Type: sql.Text},
	{Name: "db", Type: sql.Text},
	{Name: "Command", Type: sql.Text},
	{Name: "Time", Type: sql.Int64},
	{Name: "State", Type: sql.Text},
	{Name: "Info", Type: sql.Text},
}

// ShowProcessList shows a list of all current running processes.
type ShowProcessList struct {
	Database string
	*sql.ProcessList
}

// NewShowProcessList creates a new ProcessList node.
func NewShowProcessList() *ShowProcessList { return new(ShowProcessList) }

// Children implements the Node interface.
func (p *ShowProcessList) Children() []sql.Node { return nil }

// Resolved implements the Node interface.
func (p *ShowProcessList) Resolved() bool { return true }

// TransformUp implements the Node interface.
func (p *ShowProcessList) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(p)
}

// TransformExpressionsUp implements the Node interface.
func (p *ShowProcessList) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return p, nil
}

// Schema implements the Node interface.
func (p *ShowProcessList) Schema() sql.Schema { return processListSchema }

// RowIter implements the Node interface.
func (p *ShowProcessList) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	processes := p.Processes()
	var rows = make([]sql.Row, len(processes))

	for i, proc := range processes {
		var status []string
		for name, progress := range proc.Progress {
			status = append(status, fmt.Sprintf("%s(%s)", name, progress))
		}

		if len(status) == 0 {
			status = []string{"running"}
		}

		sort.Strings(status)

		rows[i] = process{
			id:      int64(proc.Pid),
			user:    proc.User,
			time:    int64(proc.Seconds()),
			state:   strings.Join(status, ", "),
			command: proc.Type.String(),
			host:    ctx.Session.Address(),
			info:    proc.Query,
			db:      p.Database,
		}.toRow()
	}

	return sql.RowsToRowIter(rows...), nil
}

func (p *ShowProcessList) String() string { return "ProcessList" }
