package command

import (
	"reflect"
	"testing"

	"github.com/maltemindedal/stash/internal/server"
)

func TestNewRefusesServicesWithACollaboratorMissing(t *testing.T) {
	// Every pointer and function field of Services is a collaborator the
	// executor uses without checking for nil, so each one is required. The
	// loop covers a field added later as well.
	fields := reflect.TypeOf(server.Services{})
	for i := 0; i < fields.NumField(); i++ {
		field := fields.Field(i)
		switch field.Type.Kind() {
		case reflect.Pointer, reflect.Func, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan:
		default:
			continue
		}
		i := i // the closure keeps its own field index (go 1.21 shares the loop variable)
		t.Run(field.Name, func(t *testing.T) {
			services := testServices()
			reflect.ValueOf(&services).Elem().Field(i).Set(reflect.Zero(field.Type))

			executor, err := New(services)
			if want := "command: missing Services." + field.Name; err == nil || err.Error() != want {
				t.Fatalf("New() = (%v, %v), want the error %q", executor, err, want)
			}
		})
	}
}

func TestNewAcceptsTheZeroValueOfEverySetting(t *testing.T) {
	// RequirePass and SlowlogThreshold are settings, not collaborators: no
	// password and a Slowlog that records every command are valid.
	services := testServices()
	services.RequirePass = ""
	services.SlowlogThreshold = 0

	if _, err := New(services); err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
}
