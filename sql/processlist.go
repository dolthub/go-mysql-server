package sql

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gopkg.in/src-d/go-errors.v1"
)

// Progress between done items and total items.
type Progress struct {
	Done  int64
	Total int64
}

func (p Progress) String() string {
	var total = "?"
	if p.Total > 0 {
		total = fmt.Sprint(p.Total)
	}

	return fmt.Sprintf("%d/%s", p.Done, total)
}

// ProcessType is the type of process.
type ProcessType byte

const (
	// QueryProcess is a query process.
	QueryProcess ProcessType = iota
	// CreateIndexProcess is a process to create an index.
	CreateIndexProcess
)

func (p ProcessType) String() string {
	switch p {
	case QueryProcess:
		return "query"
	case CreateIndexProcess:
		return "create_index"
	default:
		return "invalid"
	}
}

// Process represents a process in the SQL server.
type Process struct {
	Pid        uint64
	Connection uint32
	User       string
	Type       ProcessType
	Query      string
	Progress   map[string]Progress
	StartedAt  time.Time
	Kill       context.CancelFunc
}

// Done needs to be called when this process has finished.
func (p *Process) Done() { p.Kill() }

// Seconds returns the number of seconds this process has been running.
func (p *Process) Seconds() uint64 {
	return uint64(time.Since(p.StartedAt) / time.Second)
}

// ProcessList is a structure that keeps track of all the processes and their
// status.
type ProcessList struct {
	mu    sync.RWMutex
	procs map[uint64]*Process
}

// NewProcessList creates a new process list.
func NewProcessList() *ProcessList {
	return &ProcessList{
		procs: make(map[uint64]*Process),
	}
}

// ErrPidAlreadyUsed is returned when the pid is already registered.
var ErrPidAlreadyUsed = errors.NewKind("pid %d is already in use")

// AddProcess adds a new process to the list given a process type and a query.
// Steps is a map between the name of the items that need to be completed and
// the total amount in these items. -1 means unknown.
// It returns a new context that should be passed around from now on. That
// context will be cancelled if the process is killed.
func (pl *ProcessList) AddProcess(
	ctx *Context,
	typ ProcessType,
	query string,
) (*Context, error) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	if _, ok := pl.procs[ctx.Pid()]; ok {
		return nil, ErrPidAlreadyUsed.New(ctx.Pid())
	}

	newCtx, cancel := context.WithCancel(ctx)
	ctx = ctx.WithContext(newCtx)

	pl.procs[ctx.Pid()] = &Process{
		Pid:        ctx.Pid(),
		Connection: ctx.ID(),
		Type:       typ,
		Query:      query,
		Progress:   make(map[string]Progress),
		User:       ctx.Session.User(),
		StartedAt:  time.Now(),
		Kill:       cancel,
	}

	return ctx, nil
}

// UpdateProgress updates the progress of the item with the given name for the
// process with the given pid.
func (pl *ProcessList) UpdateProgress(pid uint64, name string, delta int64) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	p, ok := pl.procs[pid]
	if !ok {
		return
	}

	progress, ok := p.Progress[name]
	if !ok {
		progress = Progress{Total: -1}
	}

	progress.Done += delta
	p.Progress[name] = progress
}

// AddProgressItem adds a new item to track progress from to the proces with
// the given pid. If the pid does not exist, it will do nothing.
func (pl *ProcessList) AddProgressItem(pid uint64, name string, total int64) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	p, ok := pl.procs[pid]
	if !ok {
		return
	}

	if pg, ok := p.Progress[name]; ok {
		pg.Total = total
		p.Progress[name] = pg
	} else {
		p.Progress[name] = Progress{Total: total}
	}
}

// Kill terminates a process if it exists.
func (pl *ProcessList) Kill(pid uint64) {
	pl.Done(pid)
}

// KillConnection kills all processes that have the same connection as the one
// of the process with the given process id. If the process does not exist, it
// will do nothing.
func (pl *ProcessList) KillConnection(pid uint64) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	proc, ok := pl.procs[pid]
	if !ok {
		return
	}

	conn := proc.Connection
	for pid, proc := range pl.procs {
		if proc.Connection == conn {
			proc.Kill()
			delete(pl.procs, pid)
		}
	}
}

// Done removes the finished process with the given pid from the process list.
// If the process does not exist, it will do nothing.
func (pl *ProcessList) Done(pid uint64) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	if proc, ok := pl.procs[pid]; ok {
		proc.Done()
	}

	delete(pl.procs, pid)
}

// Processes returns the list of current running processes.
func (pl *ProcessList) Processes() []Process {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	var result = make([]Process, 0, len(pl.procs))

	for _, proc := range pl.procs {
		p := *proc
		var progress = make(map[string]Progress, len(p.Progress))
		for n, p := range p.Progress {
			progress[n] = p
		}
		result = append(result, p)
	}

	return result
}
