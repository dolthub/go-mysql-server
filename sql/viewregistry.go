package sql

import (
	"strings"
	"sync"

	"gopkg.in/src-d/go-errors.v1"
)

var (
	ErrExistingView = errors.NewKind("the view %s.%s already exists in the registry")
	ErrNonExistingView = errors.NewKind("the view %s.%s does not exist in the registry")
)

type View struct {
	Name       string
	Definition Node
}

type viewKey struct {
	dbName, viewName string
}

// Creates a viewKey ensuring both names are lowercase
func newViewKey(databaseName, viewName string) viewKey {
	return viewKey{strings.ToLower(databaseName), strings.ToLower(viewName)}
}

type ViewRegistry struct {
	mutex sync.RWMutex
	views map[viewKey]View
}

func NewViewRegistry() *ViewRegistry {
	return &ViewRegistry{
		views: make(map[viewKey]View),
	}
}

func (registry *ViewRegistry) Register(database string, view View) error {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()

	key := newViewKey(database, view.Name)

	if _, ok := registry.views[key]; ok {
		return ErrExistingView.New(database, view.Name)
	}

	registry.views[key] = view
	return nil
}

// Deletes the view specified by the pair {databaseName, viewName}, returning
// an error if it does not exist
func (registry *ViewRegistry) Delete(databaseName, viewName string) error {
	key := newViewKey(databaseName, viewName)

	if _, ok := registry.views[key]; !ok {
		return ErrNonExistingView.New(databaseName, viewName)
	}

	delete(registry.views, key)
	return nil
}

func (registry *ViewRegistry) View(databaseName, viewName string) (*View, error) {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()

	key := newViewKey(databaseName, viewName)

	if view, ok := registry.views[key]; ok {
		return &view, nil
	}

	return nil, ErrNonExistingView.New(databaseName, viewName)
}

func (registry *ViewRegistry) ViewsInDatabase(databaseName string) (views []View) {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()

	for key, value := range registry.views {
		if key.dbName == databaseName {
			views = append(views, value)
		}
	}

	return views
}
