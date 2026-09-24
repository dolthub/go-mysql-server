package sql

import (
	"context"
	"testing"
)

type postgresSettingTestSession struct {
	*BaseSession
	name  string
	value any
	local bool
}

func (s *postgresSettingTestSession) SetPostgresSetting(_ *Context, name string, value any, local bool) error {
	s.name, s.value, s.local = name, value, local
	return nil
}

func (s *postgresSettingTestSession) GetPostgresSetting(_ *Context, name string) (any, bool, error) {
	return s.value, name == s.name, nil
}

func TestPostgresSettingScopeUsesIntegrator(t *testing.T) {
	sess := &postgresSettingTestSession{BaseSession: NewBaseSession()}
	ctx := NewContext(context.Background(), WithSession(sess))
	scope := PostgresSettingScope{Local: true}
	if err := scope.SetValue(ctx, "app.tenant", "a"); err != nil {
		t.Fatal(err)
	}
	if sess.name != "app.tenant" || sess.value != "a" || !sess.local {
		t.Fatal("setting scope did not pass the local value to its integrator")
	}
	value, err := scope.GetValue(ctx, "app.tenant", Collation_Default)
	if err != nil || value != "a" {
		t.Fatalf("GetValue: %v, %v", value, err)
	}
	plain := NewContext(context.Background(), WithSession(NewBaseSession()))
	if err := scope.SetValue(plain, "app.tenant", "a"); err == nil {
		t.Fatal("unsupported session accepted a PostgreSQL setting")
	}
}
