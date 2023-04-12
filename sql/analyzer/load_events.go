// Copyright 2023 Dolthub, Inc.
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

package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// loadEvents loads any events that are required for a plan node to operate properly (except for nodes dealing with
// event execution).
func loadEvents(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("loadEvents")
	defer span.End()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch node := n.(type) {
		case *plan.ShowEvents:
			newShowEvents := *node
			loadedEvents, err := loadEventsFromDb(ctx, newShowEvents.Database())
			if err != nil {
				return nil, transform.SameTree, err
			}
			newShowEvents.Events = loadedEvents
			return &newShowEvents, transform.NewTree, nil
		default:
			return node, transform.SameTree, nil
		}
	})
}

func loadEventsFromDb(ctx *sql.Context, db sql.Database) ([]sql.EventDetails, error) {
	var loadedEvents []sql.EventDetails
	if eventDb, ok := db.(sql.EventDatabase); ok {
		events, err := eventDb.GetEvents(ctx)
		if err != nil {
			return nil, err
		}
		for _, event := range events {
			parsedCreateEvent, err := parse.Parse(ctx, event.CreateStatement)
			if err != nil {
				return nil, err
			}
			eventPlan, ok := parsedCreateEvent.(*plan.CreateEvent)
			if !ok {
				return nil, sql.ErrEventCreateStatementInvalid.New(event.CreateStatement)
			}
			ed, err := eventPlan.GetEventDetails(ctx, event.CreatedAt)
			if err != nil {
				return nil, err
			}

			loadedEvents = append(loadedEvents, ed)
		}
	}
	return loadedEvents, nil
}
