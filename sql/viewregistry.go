package sql

import (
	"strings"
	"sync"

	"gopkg.in/src-d/go-errors.v1"
)

var (
	ErrExistingView    = errors.NewKind("the view %s.%s already exists in the registry")
	ErrNonExistingView = errors.NewKind("the view %s.%s does not exist in the registry")
)

// View is defined by a Node and has a name.
type View struct {
	name       string
	definition Node
}

// NewView creates a View with the specified name and definition.
func NewView(name string, definition Node) View {
	return View{name, definition}
}

// Name returns the name of the view.
func (v *View) Name() string {
	return v.name
}

// Definition returns the definition of the view.
func (v *View) Definition() Node {
	return v.definition
}

// Views are scoped by the databases in which they were defined, so a key in
// the view registry is a pair of names: database and view.
type viewKey struct {
	dbName, viewName string
}

// newViewKey creates a viewKey ensuring both names are lowercase.
func newViewKey(databaseName, viewName string) viewKey {
	return viewKey{strings.ToLower(databaseName), strings.ToLower(viewName)}
}

// ViewRegistry is a map of viewKey to View whose access is protected by a
// RWMutex.
type ViewRegistry struct {
	mutex sync.RWMutex
	views map[viewKey]View
}

// NewViewRegistry creates an empty ViewRegistry.
func NewViewRegistry() *ViewRegistry {
	return &ViewRegistry{
		views: make(map[viewKey]View),
	}
}

// Register adds the view specified by the pair {database, view.Name()},
// returning an error if there is already an element with that key.
func (r *ViewRegistry) Register(database string, view View) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	key := newViewKey(database, view.Name())

	if _, ok := r.views[key]; ok {
		return ErrExistingView.New(database, view.Name())
	}

	r.views[key] = view
	return nil
}

// Delete deletes the view specified by the pair {databaseName, viewName},
// returning an error if it does not exist.
func (r *ViewRegistry) Delete(databaseName, viewName string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	key := newViewKey(databaseName, viewName)

	if _, ok := r.views[key]; !ok {
		return ErrNonExistingView.New(databaseName, viewName)
	}

	delete(r.views, key)
	return nil
}

// View returns a pointer to the view specified by the pair {databaseName,
// viewName}, returning an error if it does not exist.
func (r *ViewRegistry) View(databaseName, viewName string) (*View, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	key := newViewKey(databaseName, viewName)

	if view, ok := r.views[key]; ok {
		return &view, nil
	}

	return nil, ErrNonExistingView.New(databaseName, viewName)
}

// AllViews returns the map of all views in the registry.
func (r *ViewRegistry) AllViews() map[viewKey]View {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return r.views
}

// ViewsInDatabase returns an array of all the views registered under the
// specified database.
func (r *ViewRegistry) ViewsInDatabase(databaseName string) (views []View) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	for key, value := range r.views {
		if key.dbName == databaseName {
			views = append(views, value)
		}
	}

	return views
}
