//go:build linux || darwin || freebsd

package server

import (
	"errors"
	"net"
	"syscall"
)

// peerClosed reports whether the peer of conn has closed its side of the
// connection and left nothing unread. It looks at the socket without consuming
// anything, so requests the client has already sent stay where they are, and it
// never waits.
func peerClosed(conn net.Conn) bool {
	syscallConn, ok := conn.(syscall.Conn)
	if !ok {
		return false
	}
	raw, err := syscallConn.SyscallConn()
	if err != nil {
		return false
	}

	closed := false
	var probe [1]byte
	err = raw.Read(func(fd uintptr) bool {
		n, _, recvErr := syscall.Recvfrom(int(fd), probe[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
		switch {
		case recvErr == nil:
			// Data waiting means the client is still there; nothing waiting and a
			// zero-length read means it sent FIN.
			closed = n == 0
		case errors.Is(recvErr, syscall.EAGAIN), errors.Is(recvErr, syscall.EINTR):
			// Open, and nothing to read.
		default:
			// A reset or another socket error.
			closed = true
		}
		return true
	})
	if err != nil {
		return errors.Is(err, net.ErrClosed)
	}
	return closed
}
