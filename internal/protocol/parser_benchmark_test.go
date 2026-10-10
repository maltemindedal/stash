package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"testing"
)

func BenchmarkParserParse(b *testing.B) {
	benchmarks := []struct {
		name  string
		value Value
	}{
		{
			name: "PING",
			value: Array{Elements: []Value{
				BulkString{Data: []byte("PING")},
			}},
		},
		{
			name: "SET with PX",
			value: Array{Elements: []Value{
				BulkString{Data: []byte("SET")},
				BulkString{Data: []byte("key")},
				BulkString{Data: []byte("value")},
				BulkString{Data: []byte("PX")},
				BulkString{Data: []byte("1000")},
			}},
		},
		{
			name: "Nested array",
			value: Array{Elements: []Value{
				BulkString{Data: []byte("CMD")},
				Array{Elements: []Value{
					BulkString{Data: []byte("one")},
					BulkString{Data: []byte("two")},
				}},
			}},
		},
	}

	for _, bm := range benchmarks {
		bm := bm
		raw, err := Encode(bm.value)
		if err != nil {
			b.Fatalf("Encode(%s) error = %v", bm.name, err)
		}

		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))

			for i := 0; i < b.N; i++ {
				parser := NewParser(bytes.NewReader(raw))
				if _, err := parser.Parse(); err != nil {
					b.Fatalf("Parse() error = %v", err)
				}
			}
		})
	}
}

func BenchmarkParserParseFragmentedArray(b *testing.B) {
	chunks := [][]byte{
		[]byte("*2\r\n$4\r\nEC"),
		[]byte("HO\r\n$5\r\nhe"),
		[]byte("llo\r\n"),
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		parser := NewParser(&benchmarkChunkReader{chunks: cloneChunks(chunks)})
		if _, err := parser.Parse(); err != nil {
			b.Fatalf("Parse() error = %v", err)
		}
	}
}

func BenchmarkParserParseParallel(b *testing.B) {
	benchmarks := []struct {
		name string
		raw  []byte
	}{
		{
			name: "PING",
			raw:  mustEncodeBenchValue(b, Array{Elements: []Value{BulkString{Data: []byte("PING")}}}),
		},
		{
			name: "SET with PX",
			raw: mustEncodeBenchValue(b, Array{Elements: []Value{
				BulkString{Data: []byte("SET")},
				BulkString{Data: []byte("key")},
				BulkString{Data: []byte("value")},
				BulkString{Data: []byte("PX")},
				BulkString{Data: []byte("1000")},
			}}),
		},
		{
			name: "RESP3 nested array",
			raw: mustEncodeBenchValue(b, Array{Elements: []Value{
				BulkString{Data: []byte("CMD")},
				Array{Elements: []Value{Boolean{Value: true}, Null{}}},
			}}),
		},
	}

	for _, bm := range benchmarks {
		bm := bm
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(bm.raw)))

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					parser := NewParser(bytes.NewReader(bm.raw))
					if _, err := parser.Parse(); err != nil {
						b.Fatalf("Parse() error = %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkParserParseReuseBufferedReader(b *testing.B) {
	raw := mustEncodeBenchValue(b, Array{Elements: []Value{
		BulkString{Data: []byte("SET")},
		BulkString{Data: []byte("key")},
		BulkString{Data: []byte("value")},
	}})

	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	reader := bytes.NewReader(raw)
	buffered := bufio.NewReader(reader)
	parser := NewParser(buffered)

	for i := 0; i < b.N; i++ {
		reader.Reset(raw)
		buffered.Reset(reader)
		if _, err := parser.Parse(); err != nil {
			b.Fatalf("Parse() error = %v", err)
		}
	}
}

// BenchmarkParserParsePipelined reads 1000 SET commands that arrive in 4 KiB
// reads: a client that pipelines, or an AOF being replayed. One iteration is all
// 1000 Frames, so the allocations reported are for the lot.
func BenchmarkParserParsePipelined(b *testing.B) {
	const frames = 1000
	raw := bytes.Repeat(commandFrame("SET", "k", "v"), frames)

	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for i := 0; i < b.N; i++ {
		parser := NewParser(&splitReader{data: raw, sizes: []int{4096}})
		for n := 0; n < frames; n++ {
			if _, err := parser.Parse(); err != nil {
				b.Fatalf("Parse() frame %d error = %v", n, err)
			}
		}
	}
}

// BenchmarkParserParseLargeBulk reads one bulk string that arrives in 64 KiB
// reads, which is where a Parser that copies a payload more than once shows.
func BenchmarkParserParseLargeBulk(b *testing.B) {
	for _, size := range []int{1 << 20, 64 << 20} {
		size := size
		// Built the first time this size runs, so a -bench filter that skips it
		// does not pay for the 64 MiB.
		var raw []byte

		b.Run(fmt.Sprintf("%d MiB", size>>20), func(b *testing.B) {
			if raw == nil {
				raw = append([]byte(fmt.Sprintf("$%d\r\n", size)), bytes.Repeat([]byte{'v'}, size)...)
				raw = append(raw, "\r\n"...)
			}
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for i := 0; i < b.N; i++ {
				parser := NewParser(&splitReader{data: raw, sizes: []int{64 << 10}})
				value, err := parser.Parse()
				if err != nil {
					b.Fatalf("Parse() error = %v", err)
				}
				if bulk, ok := value.(BulkString); !ok || len(bulk.Data) != size {
					b.Fatalf("Parse() = %T, want a BulkString of %d bytes", value, size)
				}
			}
		})
	}
}

type benchmarkChunkReader struct {
	chunks [][]byte
}

func (r *benchmarkChunkReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}

	chunk := r.chunks[0]
	n := copy(p, chunk)
	if n == len(chunk) {
		r.chunks = r.chunks[1:]
	} else {
		r.chunks[0] = chunk[n:]
	}

	return n, nil
}

func cloneChunks(chunks [][]byte) [][]byte {
	copied := make([][]byte, len(chunks))
	for i, chunk := range chunks {
		copied[i] = append([]byte(nil), chunk...)
	}
	return copied
}

func mustEncodeBenchValue(b *testing.B, value Value) []byte {
	b.Helper()

	raw, err := Encode(value)
	if err != nil {
		b.Fatalf("Encode(%T) error = %v", value, err)
	}

	return raw
}
