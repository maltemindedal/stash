package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// commandFrame encodes one command as an array of bulk strings.
func commandFrame(args ...string) []byte {
	var frame bytes.Buffer
	fmt.Fprintf(&frame, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&frame, "$%d\r\n%s\r\n", len(arg), arg)
	}
	return frame.Bytes()
}

func TestUnauthenticatedLimitsAllowWhatAnUnauthenticatedClientNeeds(t *testing.T) {
	cases := map[string][]byte{
		"AUTH":                      commandFrame("AUTH", "secret"),
		"PING":                      commandFrame("PING"),
		"PING with a payload":       commandFrame("PING", "hello"),
		"10 elements":               commandFrame("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"),
		"a 16 KiB bulk string":      commandFrame("AUTH", strings.Repeat("p", 16*1024)),
		"an empty array":            []byte("*0\r\n"),
		"null values":               []byte("*2\r\n$-1\r\n*-1\r\n"),
		"a simple string":           []byte("+OK\r\n"),
		"an empty bulk string":      commandFrame("PING", ""),
		"a bulk string of 16383 B.": commandFrame("AUTH", strings.Repeat("p", 16*1024-1)),
	}

	for name, frame := range cases {
		t.Run(name, func(t *testing.T) {
			parser := NewParser(bytes.NewReader(frame))
			parser.SetLimits(UnauthenticatedLimits)
			if _, err := parser.Parse(); err != nil {
				t.Fatalf("Parser.Parse() error = %v, want the frame accepted", err)
			}

			var decoder Decoder
			decoder.SetLimits(UnauthenticatedLimits)
			if _, n, err := decoder.Decode(frame); err != nil || n != len(frame) {
				t.Fatalf("Decoder.Decode() = (%d, %v), want the whole frame accepted", n, err)
			}
		})
	}
}

func TestUnauthenticatedLimitsRejectLargerFrames(t *testing.T) {
	cases := []struct {
		name    string
		frame   []byte
		wantErr string
	}{
		{name: "11 elements", frame: commandFrame("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"), wantErr: "array length 11 exceeds 10 element limit"},
		{name: "a bulk string of 16 KiB + 1", frame: commandFrame("AUTH", strings.Repeat("p", 16*1024+1)), wantErr: "bulk string length 16385 exceeds 16384 byte limit"},
		// The header alone is enough: neither implementation waits for the payload.
		{name: "a bulk header declaring 100 MiB", frame: []byte("*1\r\n$104857600\r\n"), wantErr: "bulk string length 104857600 exceeds 16384 byte limit"},
		{name: "an array header declaring a million elements", frame: []byte("*1000000\r\n"), wantErr: "array length 1000000 exceeds 10 element limit"},
		{name: "a nested array over the limit", frame: []byte("*1\r\n*11\r\n"), wantErr: "array length 11 exceeds 10 element limit"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(bytes.NewReader(tt.frame))
			parser.SetLimits(UnauthenticatedLimits)
			_, err := parser.Parse()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) || !strings.Contains(err.Error(), "has not authenticated") {
				t.Fatalf("Parser.Parse() error = %v, want %q for an unauthenticated client", err, tt.wantErr)
			}

			var decoder Decoder
			decoder.SetLimits(UnauthenticatedLimits)
			_, _, err = decoder.Decode(tt.frame)
			if err == nil || errors.Is(err, ErrIncomplete) || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Decoder.Decode() error = %v, want a permanent error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestLimitsDefaultsAreUnchanged(t *testing.T) {
	// The same frames are fine without limits, and after they are lifted again.
	big := commandFrame("SET", "key", strings.Repeat("v", 100*1024))
	many := commandFrame(append([]string{"RPUSH", "list"}, strings.Split(strings.Repeat("x,", 50), ",")[:50]...)...)

	for name, frame := range map[string][]byte{"a 100 KiB value": big, "52 elements": many} {
		t.Run(name, func(t *testing.T) {
			parser := NewParser(bytes.NewReader(append(bytes.Clone(frame), frame...)))
			if _, err := parser.Parse(); err != nil {
				t.Fatalf("Parse() without limits error = %v", err)
			}
			parser.SetLimits(UnauthenticatedLimits)
			parser.SetLimits(Limits{})
			if _, err := parser.Parse(); err != nil {
				t.Fatalf("Parse() after clearing the limits error = %v", err)
			}

			var decoder Decoder
			decoder.SetLimits(UnauthenticatedLimits)
			decoder.SetLimits(Limits{})
			if _, _, err := decoder.Decode(frame); err != nil {
				t.Fatalf("Decode() after clearing the limits error = %v", err)
			}
		})
	}
}
