package memory

import (
	"reflect"
	"testing"
)

func TestProviderHasPointerReceiver(t *testing.T) {

	provider := &memoryDBProvider{}
	ref := reflect.ValueOf(provider).Elem().Type()

	for i := 0; i < ref.NumMethod(); i++ {
		method := ref.Method(i)
		if method.IsExported() {
			function := method.Func
			if function.Type().NumIn() > 0 {
				firstArg := function.Type().In(0)
				if firstArg.Kind() != reflect.Ptr {
					t.Errorf("method: memoryDBProvider.%s doesn't have a pointer receiver", method.Name)
				}
			}
		}
	}
}
