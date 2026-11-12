package output

import (
	"fmt"
	"strings"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server/output/internal/ledger"
)

// TestTheEventLoopHoldsTheFullResyncReplyOfTheLargestSnapshotAMasterServes
// asks the server's own limits about the largest reply a request can be owed
// for one value: FULLRESYNC and a snapshot of exactly the bulk string limit,
// which a master serves in the default mode. It counts bytes and holds none.
// The reply limit used to equal the bulk string limit, so the framing alone
// closed the connection of a replica whose snapshot was within 70 bytes of it.
func TestTheEventLoopHoldsTheFullResyncReplyOfTheLargestSnapshotAMasterServes(t *testing.T) {
	line := "+FULLRESYNC " + strings.Repeat("0", 40) + " 0\r\n"
	header := fmt.Sprintf("$%d\r\n", protocol.BulkStringLimit)
	reply := int64(len(line)+len(header)) + protocol.BulkStringLimit + int64(len("\r\n"))

	tests := []struct {
		name  string
		ahead int64
	}{
		{name: "with nothing waiting ahead of it"},
		{name: "behind an earlier reply the peer has not read", ahead: int64(len("+OK\r\n"))},
		{name: "behind the most that may wait when a request is run", ahead: outputHighWater},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			l := ledger.New(NewShared().limits)
			if tt.ahead > 0 {
				l.Reply(tt.ahead)
			}
			if !l.Gate() {
				t.Fatalf("Gate() = false with %d bytes waiting, want the next request run", tt.ahead)
			}
			if effects := l.Reply(reply); effects.Close != nil {
				t.Fatalf("Reply(%d) behind %d bytes closes the connection: %v", reply, tt.ahead, effects.Close)
			}
		})
	}
}
