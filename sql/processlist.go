// Copyright 2020-2021 Dolthub, Inc.
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

package sql

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-errors.v1"
)

// Progress between done items and total items.
type Progress struct {
	Name  string
	Done  int64
	Total int64
}

func (p Progress) totalString() string {
	var total = "?"
	if p.Total > 0 {
		total = fmt.Sprint(p.Total)
	}
	return total
}

// TableProgress keeps track of a table progress, and for each of its partitions
type TableProgress struct {
	Progress
	PartitionsProgress map[string]PartitionProgress
}

func NewTableProgress(name string, total int64) TableProgress {
	return TableProgress{
		Progress: Progress{
			Name:  name,
			Total: total,
		},
		PartitionsProgress: make(map[string]PartitionProgress),
	}
}

func (p TableProgress) String() string {
	return fmt.Sprintf("%s (%d/%s partitions)", p.Name, p.Done, p.totalString())
}

// PartitionProgress keeps track of a partition progress
type PartitionProgress struct {
	Progress
}

func (p PartitionProgress) String() string {
	return fmt.Sprintf("%s (%d/%s rows)", p.Name, p.Done, p.totalString())
}

// Process represents a process in the SQL server.
type Process struct {
	Pid        uint64
	Connection uint32
	User       string
	Query      string
	Progress   map[string]TableProgress
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
		Query:      query,
		Progress:   make(map[string]TableProgress),
		User:       ctx.Session.Client().User,
		StartedAt:  time.Now(),
		Kill:       cancel,
	}

	return ctx, nil
}

// UpdateTableProgress updates the progress of the table with the given name for the
// process with the given pid.
func (pl *ProcessList) UpdateTableProgress(pid uint64, name string, delta int64) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	p, ok := pl.procs[pid]
	if !ok {
		return
	}

	progress, ok := p.Progress[name]
	if !ok {
		progress = NewTableProgress(name, -1)
	}

	progress.Done += delta
	p.Progress[name] = progress
}

// UpdatePartitionProgress updates the progress of the table partition with the
// given name for the process with the given pid.
func (pl *ProcessList) UpdatePartitionProgress(pid uint64, tableName, partitionName string, delta int64) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	p, ok := pl.procs[pid]
	if !ok {
		return
	}

	tablePg, ok := p.Progress[tableName]
	if !ok {
		return
	}

	partitionPg, ok := tablePg.PartitionsProgress[partitionName]
	if !ok {
		partitionPg = PartitionProgress{Progress: Progress{Name: partitionName, Total: -1}}
	}

	partitionPg.Done += delta
	tablePg.PartitionsProgress[partitionName] = partitionPg
}

// AddTableProgress adds a new item to track progress from to the process with
// the given pid. If the pid does not exist, it will do nothing.
func (pl *ProcessList) AddTableProgress(pid uint64, name string, total int64) {
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
		p.Progress[name] = NewTableProgress(name, total)
	}
}

// AddPartitionProgress adds a new item to track progress from to the process with
// the given pid. If the pid or the table does not exist, it will do nothing.
func (pl *ProcessList) AddPartitionProgress(pid uint64, tableName, partitionName string, total int64) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	p, ok := pl.procs[pid]
	if !ok {
		return
	}

	tablePg, ok := p.Progress[tableName]
	if !ok {
		return
	}

	if pg, ok := tablePg.PartitionsProgress[partitionName]; ok {
		pg.Total = total
		tablePg.PartitionsProgress[partitionName] = pg
	} else {
		tablePg.PartitionsProgress[partitionName] =
			PartitionProgress{Progress: Progress{Name: partitionName, Total: total}}
	}
}

// RemoveTableProgress removes an existing item tracking progress from the
// process with the given pid, if it exists.
func (pl *ProcessList) RemoveTableProgress(pid uint64, name string) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	p, ok := pl.procs[pid]
	if !ok {
		return
	}

	delete(p.Progress, name)
}

// RemovePartitionProgress removes an existing item tracking progress from the
// process with the given pid, if it exists.
func (pl *ProcessList) RemovePartitionProgress(pid uint64, tableName, partitionName string) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	p, ok := pl.procs[pid]
	if !ok {
		return
	}

	tablePg, ok := p.Progress[tableName]
	if !ok {
		return
	}

	delete(tablePg.PartitionsProgress, partitionName)
}

// Kill terminates all queries for a given connection id.
func (pl *ProcessList) Kill(connID uint32) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	for pid, proc := range pl.procs {
		if proc.Connection == connID {
			logrus.Infof("kill query: pid %d", pid)
			proc.Done()
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
		var progress = make(map[string]TableProgress, len(p.Progress))
		for n, p := range p.Progress {
			progress[n] = p
		}
		result = append(result, p)
	}

	return result
}
