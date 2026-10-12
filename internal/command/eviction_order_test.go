package command

import (
	"bytes"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/record"
	"github.com/maltemindedal/stash/internal/record/recordtest"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

// holdingLog is a stand-in append-only file that can hold the first append
// whose payload contains hold until release is closed.
type holdingLog struct {
	*recordtest.Log
	hold     []byte
	once     sync.Once
	held     chan struct{} // closed when the held append arrives
	released chan struct{}
}

func newHoldingLog() *holdingLog {
	return &holdingLog{Log: &recordtest.Log{}, held: make(chan struct{}), released: make(chan struct{})}
}

func (l *holdingLog) Append(payload []byte) (withhold bool, err error) {
	if l.hold != nil && bytes.Contains(payload, l.hold) {
		l.once.Do(func() {
			close(l.held)
			<-l.released
		})
	}
	return l.Log.Append(payload)
}

// loggedCommands parses what log holds into the commands it would replay.
func loggedCommands(t *testing.T, log *holdingLog) []*Request {
	t.Helper()
	parser := protocol.NewParser(bytes.NewReader(log.Bytes()))
	var commands []*Request
	for {
		value, err := parser.Parse()
		if err != nil {
			return commands
		}
		request, err := DecodeRequest(value)
		if err != nil {
			t.Fatalf("logged %#v: %v", value, err)
		}
		commands = append(commands, request)
	}
}

// liveAfterReplay replays commands' SETs and DELs, ignoring TTLs, and reports
// whether key holds a value at the end.
func liveAfterReplay(commands []*Request, key []byte) bool {
	live := false
	for _, command := range commands {
		switch command.Name {
		case "SET":
			if bytes.Equal(command.Args[0], key) {
				live = true
			}
		case "DEL":
			for _, arg := range command.Args {
				if bytes.Equal(arg, key) {
					live = false
				}
			}
		}
	}
	return live
}

// listCommands lists commands one per line, with long arguments cut.
func listCommands(commands []*Request) string {
	var out strings.Builder
	for _, command := range commands {
		out.WriteString("\n\t" + command.Name)
		for _, arg := range command.Args {
			if len(arg) > 16 {
				arg = append(arg[:16:16], "..."...)
			}
			out.WriteString(" " + string(arg))
		}
	}
	return out.String()
}

// handleInBackground runs request through Handle on its own goroutine and
// returns a channel that is closed once it has returned.
func handleInBackground(t *testing.T, executor *Executor, request protocol.Value) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := handle(clientContext(executor), executor, request, true); err != nil {
			t.Errorf("%v: %v", request, err)
		}
	}()
	return done
}

func isClosed(ch <-chan struct{}) func() bool {
	return func() bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
}

// TestEvictionsAreLoggedAfterEveryEarlierWriteOfTheEvictedKey forces a write of
// a key in another stripe between a write whose Eviction removes that key and
// the moment the Eviction's DEL reaches the log. The write must wait: logged
// first, it would be undone on replay by a DEL that came before it, and a key
// that is live in memory would be lost.
func TestEvictionsAreLoggedAfterEveryEarlierWriteOfTheEvictedKey(t *testing.T) {
	writer, _, victim := keysInStripes(t)
	big := string(bytes.Repeat([]byte("v"), 200))
	small := string(bytes.Repeat([]byte("w"), 100))

	tests := []struct {
		name string
		// evictOnNextWrite stores victim so that SET writer small evicts it, and
		// arranges for the Eviction's DEL to be held on its way to log until
		// release is called.
		evictOnNextWrite func(t *testing.T, executor *Executor, log *holdingLog) (held <-chan struct{}, release func())
	}{
		{
			name: "a memory-pressure eviction",
			evictOnNextWrite: func(t *testing.T, executor *Executor, log *holdingLog) (<-chan struct{}, func()) {
				store := executor.store
				store.ConfigureMaxMemory(1<<20, 0)
				if _, err := handle(clientContext(executor), executor, requestValue("SET", string(victim), big), true); err != nil {
					t.Fatal(err)
				}
				usedByVictim := store.UsedMemory()

				scratch := storage.NewStore()
				scratch.ConfigureMaxMemory(1<<20, 0)
				if _, err := scratch.Set(string(writer), []byte(small), 0); err != nil {
					t.Fatal(err)
				}
				// Room for the writer only once the victim is evicted, and for
				// a short victim beside the writer after that.
				store.ConfigureMaxMemory(usedByVictim+scratch.UsedMemory()-1, 0)

				// The DEL travels with the evicting write's own frames.
				log.hold = []byte("DEL")
				return log.held, func() { close(log.released) }
			},
		},
		{
			name: "a TTL eviction by the sweep that makes room for an accounted write",
			evictOnNextWrite: func(t *testing.T, executor *Executor, log *holdingLog) (<-chan struct{}, func()) {
				store := executor.store
				// Stands in for the server's listener, which records the
				// sweep's DEL itself.
				swept, proceed := make(chan struct{}), make(chan struct{})
				var once sync.Once
				store.SetExpirationListener(func(keys []string) {
					once.Do(func() {
						close(swept)
						<-proceed
					})
					payload, err := protocol.EncodeValues([]protocol.Value{record.DeleteFrame(keys)})
					if err == nil {
						_, err = log.Append(payload)
					}
					if err != nil {
						t.Error(err)
					}
				})

				store.ConfigureMaxMemory(1<<20, 0)
				expiresAt := time.Now().UnixMilli() + 1
				if _, err := handle(clientContext(executor), executor, requestValue("SET", string(victim), big, "PXAT", strconv.FormatInt(expiresAt, 10)), true); err != nil {
					t.Fatal(err)
				}
				waitFor(t, "the victim's TTL to pass", func() bool { return time.Now().UnixMilli() > expiresAt+1 })
				return swept, func() { close(proceed) }
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			log := newHoldingLog()
			executor := newTestExecutorWith(func(s *server.Services) {
				s.Recorder = record.New(server.NewReplicaStream(s.Replication, s.Replicas, s.Logger), s.Logger)
				s.Recorder.SetLog(log)
			})
			held, release := tt.evictOnNextWrite(t, executor, log)

			evicting := handleInBackground(t, executor, requestValue("SET", string(writer), small))
			waitFor(t, "the Eviction's DEL to be held", isClosed(held))
			_, finished := blocked(func() { <-handleInBackground(t, executor, requestValue("SET", string(victim), "1")) })
			release()
			waitFor(t, "the evicting write to return", isClosed(evicting))
			if !finished() {
				t.Fatal("the write of the evicted key never proceeded")
			}

			if commands := loggedCommands(t, log); !liveAfterReplay(commands, victim) {
				t.Fatalf("replaying the log loses %s, which was written after its Eviction:%s", victim, listCommands(commands))
			}
		})
	}
}
