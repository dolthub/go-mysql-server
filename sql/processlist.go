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
	"time"
)

type ProcessList interface {
	// Processes returns the list of current running processes
	Processes() []Process

	// AddProcess adds a new process to the list and returns a new context that can be used to cancel it
	AddProcess(ctx *Context, query string) (*Context, error)

	// Kill terminates all queries for a given connection id
	Kill(connID uint32)

	// Done removes the finished process with the given pid from the process list
	Done(pid uint64)

	// UpdateTableProgress updates the progress of the table with the given name for the
	// process with the given pid.
	UpdateTableProgress(pid uint64, name string, delta int64)

	// UpdatePartitionProgress updates the progress of the table partition with the
	// given name for the process with the given pid.
	UpdatePartitionProgress(pid uint64, tableName, partitionName string, delta int64)

	// AddTableProgress adds a new item to track progress from to the process with
	// the given pid. If the pid does not exist, it will do nothing.
	AddTableProgress(pid uint64, name string, total int64)

	// AddPartitionProgress adds a new item to track progress from to the process with
	// the given pid. If the pid or the table does not exist, it will do nothing.
	AddPartitionProgress(pid uint64, tableName, partitionName string, total int64)

	// RemoveTableProgress removes an existing item tracking progress from the
	// process with the given pid, if it exists.
	RemoveTableProgress(pid uint64, name string)

	// RemovePartitionProgress removes an existing partition tracking progress from the
	// process with the given pid, if it exists.
	RemovePartitionProgress(pid uint64, tableName, partitionName string)
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

// Progress between done items and total items
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

// EmptyProcessList is a no-op implementation of ProcessList suitable for use in tests or other installations that
// don't require a process list
type EmptyProcessList struct{}

var _ ProcessList = EmptyProcessList{}

func (e EmptyProcessList) Processes() []Process {
	return nil
}

func (e EmptyProcessList) AddProcess(ctx *Context, query string) (*Context, error) {
	return ctx, nil
}

func (e EmptyProcessList) Kill(connID uint32)                                       {}
func (e EmptyProcessList) Done(pid uint64)                                          {}
func (e EmptyProcessList) UpdateTableProgress(pid uint64, name string, delta int64) {}
func (e EmptyProcessList) UpdatePartitionProgress(pid uint64, tableName, partitionName string, delta int64) {
}
func (e EmptyProcessList) AddTableProgress(pid uint64, name string, total int64) {}
func (e EmptyProcessList) AddPartitionProgress(pid uint64, tableName, partitionName string, total int64) {
}
func (e EmptyProcessList) RemoveTableProgress(pid uint64, name string)                         {}
func (e EmptyProcessList) RemovePartitionProgress(pid uint64, tableName, partitionName string) {}
