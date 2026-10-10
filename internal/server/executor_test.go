package server

import (
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestNewFillsEveryServicesField(t *testing.T) {
	// The executor refuses a Services with a nil collaborator, so a field added
	// to Services that New forgets to fill would stop every server from
	// starting. This catches it here instead. Value fields (RequirePass,
	// SlowlogThreshold) are not checked: their zero values are valid settings.
	var services Services
	_, err := New(config.Default(), slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), func(s Services) (CommandExecutor, error) {
		services = s
		return stubExecutor{}, nil
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	value := reflect.ValueOf(services)
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		switch field.Kind() {
		case reflect.Pointer, reflect.Func, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan:
		default:
			continue
		}
		name := value.Type().Field(i).Name
		t.Run(name, func(t *testing.T) {
			if field.IsNil() {
				t.Fatalf("New left Services.%s nil", name)
			}
		})
	}
}

func TestEachOriginHasOnlyItsCapabilities(t *testing.T) {
	type capabilities struct {
		propagates, recordsSlowlog, needsAuth, answersGetAck bool
	}
	tests := []struct {
		name   string
		origin Origin
		want   capabilities
	}{
		{name: "a client's request is propagated, recorded in the Slowlog and needs AUTH", origin: OriginClient, want: capabilities{propagates: true, recordsSlowlog: true, needsAuth: true}},
		{name: "the Master's stream only answers GETACK", origin: OriginMaster, want: capabilities{answersGetAck: true}},
		{name: "AOF replay has none of them", origin: OriginReplay},
		{name: "the zero Origin has none of them", origin: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := capabilities{
				propagates:     tt.origin.Propagates(),
				recordsSlowlog: tt.origin.RecordsSlowlog(),
				needsAuth:      tt.origin.NeedsAuth(),
				answersGetAck:  tt.origin.AnswersGetAck(),
			}
			if got != tt.want {
				t.Fatalf("Origin %d capabilities = %+v, want %+v", tt.origin, got, tt.want)
			}
		})
	}
}

func TestNewReportsAnExecutorItCannotBuild(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		name        string
		newExecutor NewExecutorFunc
		want        string
	}{
		{
			name:        "the constructor fails",
			newExecutor: func(Services) (CommandExecutor, error) { return nil, errors.New("command: missing Services.Store") },
			want:        "command: missing Services.Store",
		},
		{
			name:        "the constructor returns no executor",
			newExecutor: func(Services) (CommandExecutor, error) { return nil, nil },
			want:        "returned no executor",
		},
		{
			name: "there is no constructor",
			want: "no command executor constructor",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, err := New(config.Default(), logger, storage.NewStore(), tt.newExecutor)
			if err == nil || !strings.HasPrefix(err.Error(), "server: ") || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("New() = (%v, %v), want a server: error containing %q", srv, err, tt.want)
			}
		})
	}
}
