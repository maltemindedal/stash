package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// offeredBytes feeds frame to decode in chunk-sized reads, as the event loop
// does, and returns how many bytes decode was handed past the point it had
// already consumed, summed over every call. A decoder that keeps its place is
// handed each byte about once; one that rescans the frame on every read is
// handed it once per read.
func offeredBytes(t *testing.T, frame []byte, chunk int, decode func(buf []byte, resumeFrom int) (done bool, resumeAt int, err error)) int {
	t.Helper()
	offered, resume := 0, 0
	for end := chunk; ; end += chunk {
		if end > len(frame) {
			end = len(frame)
		}
		offered += end - resume
		done, next, err := decode(frame[:end], resume)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if done {
			return offered
		}
		resume = next
		if end == len(frame) {
			t.Fatalf("frame of %d bytes never completed", len(frame))
		}
	}
}

func rpushFrame(args int) []byte {
	var frame bytes.Buffer
	fmt.Fprintf(&frame, "*%d\r\n$5\r\nRPUSH\r\n$1\r\nk\r\n", args+2)
	for i := 0; i < args; i++ {
		frame.WriteString("$1\r\nx\r\n")
	}
	return frame.Bytes()
}

func TestDecoderIsHandedEachByteOfAFrameArrivingInManyReadsAboutOnce(t *testing.T) {
	frame := rpushFrame(400_000)
	var d Decoder
	offered := offeredBytes(t, frame, 64*1024, func(buf []byte, _ int) (bool, int, error) {
		_, _, err := d.Decode(buf)
		if errors.Is(err, ErrIncomplete) {
			return false, d.pos, nil
		}
		return err == nil, 0, err
	})
	if offered > 2*len(frame) {
		t.Fatalf("a %d byte frame in 64 KiB reads was handed to the decoder as %d bytes (%.1fx): want each byte about once", len(frame), offered, float64(offered)/float64(len(frame)))
	}
}
