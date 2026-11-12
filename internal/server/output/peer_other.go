//go:build !(linux || darwin || freebsd)

package output

import "net"

// peerClosed cannot look at the socket on this platform, so a blocked command
// keeps waiting for its wake-up as it always has.
func peerClosed(net.Conn) bool { return false }
