package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/record"
	"github.com/maltemindedal/stash/internal/record/recordtest"
	"github.com/maltemindedal/stash/internal/storage"
)

// setExecutor answers every request OK, with the request itself as the
// command frame for the AOF and the replicas, as a SET is.
type setExecutor struct{}

func (setExecutor) Handle(_ context.Context, call Call, request protocol.Value) (ExecuteResult, error) {
	result := SingleResponse(protocol.SimpleString{Value: "OK"})
	result.Write = record.Write{Origin: call.Origin}
	result.Write.Add(request.(protocol.Array), record.ToBoth)
	return result, nil
}

// TestAClientWriteThatCannotBeMadeDurableIsAnsweredWithPersistenceFailure
// runs a write through the pipeline both networking modes share while every
// append fails. Under appendfsync always the Recorder withholds it: the client
// is answered ERR persistence failure and the replication stream never counts
// it. Under the deferred policies the failure is only logged.
func TestAClientWriteThatCannotBeMadeDurableIsAnsweredWithPersistenceFailure(t *testing.T) {
	tests := []struct {
		name          string
		withhold      bool
		wantReply     protocol.Value
		wantPropagate bool
	}{
		{name: "always", withhold: true, wantReply: protocol.ErrorValue{Message: "ERR persistence failure"}},
		{name: "everysec or no", wantReply: protocol.SimpleString{Value: "OK"}, wantPropagate: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			srv := newTestServer(t, config.Config{}, logger, storage.NewStore(), setExecutor{})
			log := &recordtest.Log{}
			log.Fail(errors.New("no space left on device"), tt.withhold)
			srv.recorder.SetLog(log)
			state := &ClientState{ID: 1, Authenticated: true}
			call := Call{Client: state, Origin: record.OriginClient}

			responses, _, err := srv.executeClientRequest(context.Background(), call, state.ID, &stubConn{}, logger, requestFrame("SET", "k", "v"))
			if err != nil {
				t.Fatalf("executeClientRequest() error = %v", err)
			}
			if len(responses) != 1 || responses[0] != tt.wantReply {
				t.Fatalf("replies = %#v, want %#v", responses, tt.wantReply)
			}
			if propagated := srv.replication.MasterOffset() > 0; propagated != tt.wantPropagate {
				t.Fatalf("propagated = %v (master offset %d), want %v", propagated, srv.replication.MasterOffset(), tt.wantPropagate)
			}
			if log.Appends() != 1 {
				t.Fatalf("%d appends, want 1", log.Appends())
			}
		})
	}
}
