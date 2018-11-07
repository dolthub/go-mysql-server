package auth_test

import (
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0/auth"
)

func TestNoneAuthentication(t *testing.T) {
	a := new(auth.None)

	tests := []authenticationTest{
		{"root", "", true},
		{"root", "password", true},
		{"root", "mysql_password", true},
		{"user", "password", true},
		{"user", "other_password", true},
		{"user", "", true},
		{"", "", true},
		{"", "password", true},
	}

	testAuthentication(t, a, tests, nil)
}

func TestNoneAuthorization(t *testing.T) {
	a := new(auth.None)

	tests := []authorizationTest{
		{"user", queries["select"], true},
		{"root", queries["select"], true},
		{"", queries["select"], true},

		{"user", queries["create_index"], true},

		{"root", queries["drop_index"], true},

		{"", queries["insert"], true},

		{"user", queries["lock"], true},

		{"root", queries["unlock"], true},
	}

	testAuthorization(t, a, tests, nil)
}
