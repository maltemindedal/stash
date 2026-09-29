package server

import (
	"strings"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
)

func TestObserveCommandRedactsAuthArguments(t *testing.T) {
	// MONITOR fans commands out to other clients, including AUTH attempts that
	// failed, so a password typed by any client must never reach the stream.
	tests := []struct {
		name  string
		parts []string
		want  []string
	}{
		{name: "password", parts: []string{"AUTH", "hunter2"}, want: []string{"AUTH", "[redacted]"}},
		{name: "lowercase name", parts: []string{"auth", "hunter2"}, want: []string{"AUTH", "[redacted]"}},
		{name: "every argument", parts: []string{"Auth", "user", "hunter2"}, want: []string{"AUTH", "[redacted]", "[redacted]"}},
		{name: "no arguments", parts: []string{"AUTH"}, want: []string{"AUTH"}},
		{name: "other commands stay verbatim", parts: []string{"set", "auth", "hunter2"}, want: []string{"SET", "auth", "hunter2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			elements := make([]protocol.Value, len(tt.parts))
			for i, part := range tt.parts {
				elements[i] = protocol.BulkString{Data: []byte(part)}
			}

			observed := observeCommand(protocol.Array{Elements: elements}, 7, nil)
			if !observed.OK || strings.Join(observed.Parts, " ") != strings.Join(tt.want, " ") {
				t.Fatalf("observeCommand().Parts = %q, want %q", observed.Parts, tt.want)
			}
			if tt.want[0] == "AUTH" && strings.Contains(monitorLine(observed), "hunter2") {
				t.Fatalf("monitor line %q leaks the password", monitorLine(observed))
			}
		})
	}
}
