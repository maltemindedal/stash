package test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
)

// benchmarkPipelineDepth is how many requests the pipelined benchmarks send
// before they read the replies.
const benchmarkPipelineDepth = 32

// startBenchmarkServer starts a server from cfg and stops it when b ends.
func startBenchmarkServer(b *testing.B, cfg config.Config) string {
	b.Helper()

	addr, stop, errCh := startTestServer(b, cfg)
	b.Cleanup(func() {
		stop()
		waitForServerStop(b, errCh)
	})
	return addr
}

func dialBenchmark(b *testing.B, addr string) net.Conn {
	b.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatalf("Dial(%q) error = %v", addr, err)
	}
	b.Cleanup(func() { _ = conn.Close() })
	return conn
}

// runPipelinedSets sends b.N SETs over conn, benchmarkPipelineDepth at a time,
// and reads every reply.
func runPipelinedSets(b *testing.B, conn net.Conn) {
	b.Helper()

	var pipeline bytes.Buffer
	for i := 0; i < benchmarkPipelineDepth; i++ {
		if err := protocol.WriteValue(&pipeline, request("SET", fmt.Sprintf("key:%02d", i), "value")); err != nil {
			b.Fatalf("WriteValue(SET) error = %v", err)
		}
	}
	replies := make([]byte, benchmarkPipelineDepth*len("+OK\r\n"))
	want := strings.Repeat("+OK\r\n", benchmarkPipelineDepth)

	b.ReportAllocs()
	b.ResetTimer()
	for sent := 0; sent < b.N; sent += benchmarkPipelineDepth {
		if _, err := conn.Write(pipeline.Bytes()); err != nil {
			b.Fatalf("Write(pipeline) error = %v", err)
		}
		if _, err := io.ReadFull(conn, replies); err != nil {
			b.Fatalf("ReadFull(replies) error = %v", err)
		}
		if string(replies) != want {
			b.Fatalf("replies = %q, want %d +OK", replies, benchmarkPipelineDepth)
		}
	}
	b.StopTimer()
}

// BenchmarkPipelinedSet measures one SET of a pipeline of 32 over TCP: the
// request path, and the batching of its replies.
func BenchmarkPipelinedSet(b *testing.B) {
	forEachNetworkingMode(b, defaultTestConfig(), func(b *testing.B, cfg config.Config) {
		conn := dialBenchmark(b, startBenchmarkServer(b, cfg))
		runPipelinedSets(b, conn)
	})
}

// BenchmarkPublishFanOut measures one PUBLISH to 32 subscribers over TCP, from
// the request to its reply, and waits for every subscriber to have received
// every message before it stops the clock.
func BenchmarkPublishFanOut(b *testing.B) {
	const subscribers = 32
	const message = "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n"

	forEachNetworkingMode(b, defaultTestConfig(), func(b *testing.B, cfg config.Config) {
		addr := startBenchmarkServer(b, cfg)

		var received atomic.Int64
		var readers sync.WaitGroup
		for i := 0; i < subscribers; i++ {
			conn := dialBenchmark(b, addr)
			reader := bufio.NewReader(conn)
			if err := protocol.WriteValue(conn, request("SUBSCRIBE", "news")); err != nil {
				b.Fatalf("WriteValue(SUBSCRIBE) error = %v", err)
			}
			if _, err := protocol.NewParser(reader).Parse(); err != nil {
				b.Fatalf("Parse(subscribe acknowledgement) error = %v", err)
			}
			readers.Add(1)
			go func() {
				defer readers.Done()
				buf := make([]byte, 64<<10)
				for {
					n, err := reader.Read(buf)
					received.Add(int64(n))
					if err != nil {
						return
					}
				}
			}()
		}

		publisher := dialBenchmark(b, addr)
		var publish bytes.Buffer
		if err := protocol.WriteValue(&publish, request("PUBLISH", "news", "hello")); err != nil {
			b.Fatalf("WriteValue(PUBLISH) error = %v", err)
		}
		want := fmt.Sprintf(":%d\r\n", subscribers)
		reply := make([]byte, len(want))

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := publisher.Write(publish.Bytes()); err != nil {
				b.Fatalf("Write(PUBLISH) error = %v", err)
			}
			if _, err := io.ReadFull(publisher, reply); err != nil {
				b.Fatalf("ReadFull(PUBLISH reply) error = %v", err)
			}
			if string(reply) != want {
				b.Fatalf("PUBLISH reply = %q, want %q", reply, want)
			}
		}
		wantBytes := int64(b.N) * subscribers * int64(len(message))
		for deadline := time.Now().Add(30 * time.Second); received.Load() < wantBytes; {
			if time.Now().After(deadline) {
				b.Fatalf("subscribers received %d bytes of messages, want %d", received.Load(), wantBytes)
			}
			time.Sleep(50 * time.Microsecond)
		}
		b.StopTimer()
	})
}

// BenchmarkSetWithAReplica measures one SET of a pipeline of 32 over TCP on a
// master with one replica attached: the request path plus the propagation of
// every write to the replica's connection.
func BenchmarkSetWithAReplica(b *testing.B) {
	forEachNetworkingMode(b, defaultTestConfig(), func(b *testing.B, cfg config.Config) {
		masterAddr := startBenchmarkServer(b, cfg)

		replicaCfg := defaultTestConfig()
		replicaCfg.ReplicaOf = masterAddr
		startBenchmarkServer(b, replicaCfg)

		conn := dialBenchmark(b, masterAddr)
		parser := protocol.NewParser(conn)
		for deadline := time.Now().Add(10 * time.Second); ; {
			if err := protocol.WriteValue(conn, request("INFO", "replication")); err != nil {
				b.Fatalf("WriteValue(INFO) error = %v", err)
			}
			info, err := parser.Parse()
			if err != nil {
				b.Fatalf("Parse(INFO) error = %v", err)
			}
			if text, _, ok := integrationBulkStringContent(info); ok && strings.Contains(text, "connected_slaves:1") {
				break
			}
			if time.Now().After(deadline) {
				b.Fatal("the replica did not attach within 10s")
			}
			time.Sleep(5 * time.Millisecond)
		}

		runPipelinedSets(b, conn)
	})
}
