package test

import (
	"fmt"
	"testing"

	"github.com/maltemindedal/stash/internal/config"
)

// TestABulkLengthOverTheLimitGetsTheSameReplyInBothModes sends only the header of
// a SET whose value declares more than the 512 MiB bulk limit. Both networking
// modes answer as soon as the header arrives, with the same error, and close.
// The event loop once answered the first header with its read-buffer error and
// never answered the second, which it buffered until 512 MiB had arrived.
func TestABulkLengthOverTheLimitGetsTheSameReplyInBothModes(t *testing.T) {
	forEachNetworkingMode(t, defaultTestConfig(), func(t *testing.T, cfg config.Config) {
		addr, stop, errCh := startTestServer(t, cfg)
		defer func() {
			stop()
			waitForServerStop(t, errCh)
		}()

		for _, length := range []string{"536870913", "9223372036854775807"} {
			t.Run("length "+length, func(t *testing.T) {
				reply := readUntilClosed(t, addr, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$"+length+"\r\n")
				want := fmt.Sprintf("-ERR protocol: parse array element 2: protocol: bulk string length %s exceeds 536870912 byte limit\r\n", length)
				if reply != want {
					t.Fatalf("reply = %q, want %q", reply, want)
				}
			})
		}
	})
}
