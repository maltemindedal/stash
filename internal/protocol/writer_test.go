package protocol

import "testing"

func TestEncodedLenMatchesEncode(t *testing.T) {
	tests := []struct {
		name  string
		value Value
	}{
		{name: "simple string", value: SimpleString{Value: "OK"}},
		{name: "error", value: ErrorValue{Message: "ERR boom"}},
		{name: "integer", value: Integer{Value: -42}},
		{name: "bulk string", value: BulkString{Data: []byte("Stash")}},
		{name: "null bulk string", value: BulkString{Null: true}},
		{name: "text bulk string", value: TextBulkString{Value: "hello"}},
		{name: "array", value: Array{Elements: []Value{BulkString{Data: []byte("SET")}, BulkString{Data: []byte("name")}, BulkString{Data: []byte("Stash")}}}},
		{name: "nested array", value: Array{Elements: []Value{BulkString{Data: []byte("CMD")}, Array{Elements: []Value{Boolean{Value: true}, Null{}}}}}},
		{name: "null array", value: Array{Null: true}},
		{name: "boolean", value: Boolean{Value: true}},
		{name: "null", value: Null{}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			raw, err := Encode(tt.value)
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}

			size, err := EncodedLen(tt.value)
			if err != nil {
				t.Fatalf("EncodedLen() error = %v", err)
			}
			if len(raw) != size {
				t.Fatalf("len(Encode()) = %d, want %d", len(raw), size)
			}
		})
	}
}

func TestEncodedValuesLenMatchesEncodeValues(t *testing.T) {
	values := []Value{
		SimpleString{Value: "OK"},
		Integer{Value: 7},
		BulkString{Data: []byte("Stash")},
		Array{Elements: []Value{Boolean{Value: false}, Null{}}},
	}

	raw, err := EncodeValues(values)
	if err != nil {
		t.Fatalf("EncodeValues() error = %v", err)
	}

	size, err := EncodedValuesLen(values)
	if err != nil {
		t.Fatalf("EncodedValuesLen() error = %v", err)
	}
	if len(raw) != size {
		t.Fatalf("len(EncodeValues()) = %d, want %d", len(raw), size)
	}
}

func TestAppendValueAppendsAfterDst(t *testing.T) {
	set := Array{Elements: []Value{BulkString{Data: []byte("SET")}, BulkString{Data: []byte("k")}, BulkString{Data: []byte("v")}}}
	want, err := Encode(set)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	got, err := AppendValue([]byte("prefix"), set)
	if err != nil {
		t.Fatalf("AppendValue() error = %v", err)
	}
	if string(got) != "prefix"+string(want) {
		t.Fatalf("AppendValue() = %q, want %q", got, "prefix"+string(want))
	}
}

func TestAppendValueLeavesDstsLengthOnError(t *testing.T) {
	bad := Array{Elements: []Value{BulkString{Data: []byte("SET")}, unsupported{}}}

	got, err := AppendValue([]byte("prefix"), bad)
	if err == nil {
		t.Fatal("AppendValue() error = nil, want an unsupported value type")
	}
	if string(got) != "prefix" {
		t.Fatalf("AppendValue() = %q, want %q", got, "prefix")
	}
}

// unsupported is a Value the encoder does not know.
type unsupported struct{}

func (unsupported) value() {}

func BenchmarkEncode(b *testing.B) {
	benchmarks := []struct {
		name  string
		value Value
	}{
		{
			name:  "PING",
			value: Array{Elements: []Value{BulkString{Data: []byte("PING")}}},
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
			name: "RESP3 nested array",
			value: Array{Elements: []Value{
				BulkString{Data: []byte("CMD")},
				Array{Elements: []Value{Boolean{Value: true}, Null{}}},
			}},
		},
	}

	for _, bm := range benchmarks {
		bm := bm
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				raw, err := Encode(bm.value)
				if err != nil {
					b.Fatalf("Encode() error = %v", err)
				}
				if len(raw) == 0 {
					b.Fatal("Encode() returned empty payload")
				}
			}
		})
	}
}
