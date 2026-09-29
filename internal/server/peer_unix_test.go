//go:build linux || darwin || freebsd

package server

import (
	"net"
	"testing"
	"time"
)

func tcpPair(t *testing.T) (client *net.TCPConn, server net.Conn) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer func() { _ = listener.Close() }()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := listener.Accept()
		accepted <- conn
	}()
	raw, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	server = <-accepted
	t.Cleanup(func() {
		_ = raw.Close()
		_ = server.Close()
	})
	return raw.(*net.TCPConn), server
}

func waitForPeerClosed(conn net.Conn, want bool) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if peerClosed(conn) == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return peerClosed(conn) == want
}

func TestPeerClosed(t *testing.T) {
	t.Run("an idle open connection is not closed", func(t *testing.T) {
		_, server := tcpPair(t)
		if peerClosed(server) {
			t.Fatal("peerClosed() = true for an open, idle connection")
		}
	})

	t.Run("unread data means the client is still there, and is not consumed", func(t *testing.T) {
		client, server := tcpPair(t)
		if _, err := client.Write([]byte("PING")); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		time.Sleep(50 * time.Millisecond) // let the loopback write land
		if peerClosed(server) {
			t.Fatal("peerClosed() = true with data waiting")
		}
		buf := make([]byte, 4)
		if err := server.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatalf("SetReadDeadline() error = %v", err)
		}
		if n, err := server.Read(buf); err != nil || string(buf[:n]) != "PING" {
			t.Fatalf("Read() after peerClosed = (%q, %v), want the 4 bytes still there", buf[:n], err)
		}
	})

	t.Run("a client that closed is closed", func(t *testing.T) {
		client, server := tcpPair(t)
		_ = client.Close()
		if !waitForPeerClosed(server, true) {
			t.Fatal("peerClosed() = false after the client closed")
		}
	})

	t.Run("a client that half-closed with nothing left to read is closed", func(t *testing.T) {
		client, server := tcpPair(t)
		_ = client.CloseWrite()
		if !waitForPeerClosed(server, true) {
			t.Fatal("peerClosed() = false after the client shut down its writing side")
		}
	})

	t.Run("a connection this side already closed is closed", func(t *testing.T) {
		_, server := tcpPair(t)
		_ = server.Close()
		if !peerClosed(server) {
			t.Fatal("peerClosed() = false for a connection closed locally")
		}
	})

	t.Run("something that is not a socket is never reported closed", func(t *testing.T) {
		a, b := net.Pipe()
		defer func() { _ = a.Close(); _ = b.Close() }()
		if peerClosed(a) {
			t.Fatal("peerClosed() = true for a net.Pipe")
		}
	})
}
