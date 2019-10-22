package sql

import (
	"sync"

	"gopkg.in/src-d/go-errors.v1"
)

var ErrExistingView = errors.NewKind("the view %s.%s already exists in the registry")
var ErrNonExistingView = errors.NewKind("the view %s.%s does not exist in the registry")

type View struct {
	Name       string
	Definition Node
}

type viewKey struct {
	dbName, viewName string
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

	key := viewKey{database, view.Name}

	if _, ok := registry.views[key]; ok {
		return ErrExistingView.New(database, view.Name)
	}

	registry.views[key] = view
	return nil
}

func (registry *ViewRegistry) Delete(databaseName, viewName string) error {
	key := viewKey{databaseName, viewName}

	if _, ok := registry.views[key]; !ok {
		return ErrNonExistingView.New(databaseName, viewName)
	}

	delete(registry.views, key)
	return nil
}

func (registry *ViewRegistry) View(databaseName, viewName string) (*View, error) {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()

	key := viewKey{databaseName, viewName}

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
