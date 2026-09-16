package rdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/storage"
)

// TestLoaderRejectsOversizedString verifies a forged length prefix is rejected
// against the object-length cap rather than pre-allocating a multi-gigabyte
// buffer. The loader also runs on RDB payloads received from a replication
// master, so this prevents a network-triggered out-of-memory failure.
func TestLoaderRejectsOversizedString(t *testing.T) {
	var payload []byte
	payload = append(payload, fileHeader...)
	payload = append(payload, opcodeSelectDB)
	payload = appendLength(payload, 0)
	payload = append(payload, valueTypeString)
	payload = appendString(payload, []byte("key"))
	// Declare a ~4 GiB value without providing its bytes.
	payload = appendLength(payload, 4*1024*1024*1024-1)

	_, err := LoadReader(bytes.NewReader(payload), storage.NewStore())
	if err == nil {
		t.Fatal("LoadReader() error = nil, want oversized-length rejection")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("LoadReader() error = %v, want object-length-limit rejection", err)
	}
}

// TestLoaderRejectsOversizedLZFString verifies the loader rejects a forged LZF
// uncompressed length against the object-length cap before decompressLZF
// allocates the output buffer. Like the raw-string path, this is reachable over
// replication, so the cap provides the same protection for compressed values.
func TestLoaderRejectsOversizedLZFString(t *testing.T) {
	var payload []byte
	payload = append(payload, fileHeader...)
	payload = append(payload, opcodeSelectDB)
	payload = appendLength(payload, 0)
	payload = append(payload, valueTypeString)
	payload = appendString(payload, []byte("key"))
	// LZF-encoded value: tiny compressed length, but a ~600 MiB declared
	// uncompressed length that exceeds the cap before any payload is read.
	payload = append(payload, 0xC3)
	payload = appendLength(payload, 1)
	payload = appendLength(payload, 600*1024*1024)

	_, err := LoadReader(bytes.NewReader(payload), storage.NewStore())
	if err == nil {
		t.Fatal("LoadReader() error = nil, want oversized-LZF-length rejection")
	}
	if !strings.Contains(err.Error(), "uncompressed length") {
		t.Fatalf("LoadReader() error = %v, want LZF uncompressed-length rejection", err)
	}
}

func TestLoaderAcceptsSupportedVersions(t *testing.T) {
	for _, version := range []string{"REDIS0001", "REDIS0009", "REDIS0011"} {
		payload := append([]byte(version), opcodeEOF)
		payload = append(payload, make([]byte, 8)...)
		if _, err := LoadReader(bytes.NewReader(payload), storage.NewStore()); err != nil {
			t.Fatalf("LoadReader(%s) error = %v, want success", version, err)
		}
	}
}

func TestLoaderRejectsInvalidHeaders(t *testing.T) {
	for _, header := range []string{"REDIS9999", "REDISxxxx", "NOPE00011"} {
		payload := append([]byte(header), opcodeEOF)
		payload = append(payload, make([]byte, 8)...)
		if _, err := LoadReader(bytes.NewReader(payload), storage.NewStore()); !errors.Is(err, ErrInvalidHeader) {
			t.Fatalf("LoadReader(%s) error = %v, want ErrInvalidHeader", header, err)
		}
	}
}

func TestLoadReader(t *testing.T) {
	now := time.Now().UnixMilli()

	tests := []struct {
		name    string
		payload []byte
		now     int64
		assert  func(*testing.T, *storage.Store, Stats)
		wantErr error
	}{
		{
			name: "loads DB0 string keys with AUX and RESIZEDB metadata",
			payload: buildRDBPayload(
				auxField([]byte("redis-ver"), []byte("7.2.0")),
				selectDB(0),
				resizeDB(2, 1),
				stringEntry(rawString([]byte("name")), rawString([]byte("Stash"))),
			),
			now: 1,
			assert: func(t *testing.T, store *storage.Store, stats Stats) {
				t.Helper()
				if stats.LoadedKeys != 1 || stats.SkippedExpiredKeys != 0 {
					t.Fatalf("stats = %#v, want 1 loaded and 0 skipped", stats)
				}
				assertStoredString(t, store, "name", "Stash")
			},
		},
		{
			name: "supports integer and LZF string encodings",
			payload: buildRDBPayload(
				selectDB(0),
				stringEntry(rawString([]byte("answer")), int32String(42)),
				stringEntry(rawString([]byte("compressed")), lzfString([]byte("hello"))),
			),
			now: 1,
			assert: func(t *testing.T, store *storage.Store, stats Stats) {
				t.Helper()
				if stats.LoadedKeys != 2 || stats.SkippedExpiredKeys != 0 {
					t.Fatalf("stats = %#v, want 2 loaded and 0 skipped", stats)
				}
				assertStoredString(t, store, "answer", "42")
				assertStoredString(t, store, "compressed", "hello")
			},
		},
		{
			name: "loads EXPIRETIME and EXPIRETIMEMS entries and skips stale keys",
			payload: buildRDBPayload(
				selectDB(0),
				expiringStringEntrySeconds(uint32((now/1000)+10), rawString([]byte("future-seconds")), rawString([]byte("alive"))),
				expiringStringEntryMillis(uint64(now+15000), rawString([]byte("future-millis")), rawString([]byte("alive-too"))),
				expiringStringEntryMillis(uint64(now-10000), rawString([]byte("stale")), rawString([]byte("gone"))),
			),
			now: now,
			assert: func(t *testing.T, store *storage.Store, stats Stats) {
				t.Helper()
				if stats.LoadedKeys != 2 || stats.SkippedExpiredKeys != 1 {
					t.Fatalf("stats = %#v, want 2 loaded and 1 skipped", stats)
				}
				assertStoredString(t, store, "future-seconds", "alive")
				assertStoredString(t, store, "future-millis", "alive-too")
				if _, ok, err := store.Get("stale"); err != nil {
					t.Fatalf("Get(stale) error = %v", err)
				} else if ok {
					t.Fatal("Get(stale) ok = true, want false")
				}
			},
		},
		{
			name: "rejects non zero database selectors",
			payload: buildRDBPayload(
				selectDB(1),
			),
			wantErr: ErrUnsupportedDB,
		},
		{
			name: "rejects unsupported value types",
			payload: buildRDBPayload(
				selectDB(0),
				[]byte{0x01},
				rawString([]byte("letters")),
				encodeLength(1),
				rawString([]byte("a")),
			),
			wantErr: ErrUnsupportedValueType,
		},
		{
			name: "rejects unsupported opcodes",
			payload: buildRDBPayload(
				selectDB(0),
				[]byte{0xAB},
			),
			wantErr: ErrUnsupportedOpcode,
		},
		{
			name:    "rejects invalid header",
			payload: append([]byte("NOTREDIS11"), opcodeEOF),
			wantErr: ErrInvalidHeader,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			store := storage.NewStore()
			stats, err := loadReaderAt(bytes.NewReader(tt.payload), store, func() int64 { return tt.now })
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("loadReaderAt() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadReaderAt() error = %v", err)
			}

			tt.assert(t, store, stats)
		})
	}
}

func TestDecompressLZFRejectsCorruptPayload(t *testing.T) {
	_, err := decompressLZF([]byte{0x20}, 3)
	if err == nil {
		t.Fatal("decompressLZF() error = nil, want corruption error")
	}
}

func assertStoredString(t *testing.T, store *storage.Store, key, want string) {
	t.Helper()
	got, ok, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) error = %v", key, err)
	}
	if !ok {
		t.Fatalf("Get(%q) ok = false, want true", key)
	}
	if string(got) != want {
		t.Fatalf("Get(%q) = %q, want %q", key, string(got), want)
	}
}

func buildRDBPayload(parts ...[]byte) []byte {
	payload := append([]byte{}, []byte(fileHeader)...)
	for _, part := range parts {
		payload = append(payload, part...)
	}
	payload = append(payload, opcodeEOF)
	payload = append(payload, make([]byte, 8)...)
	return payload
}

func auxField(key, value []byte) []byte {
	payload := []byte{opcodeAux}
	payload = append(payload, rawString(key)...)
	payload = append(payload, rawString(value)...)
	return payload
}

func selectDB(index uint64) []byte {
	payload := []byte{opcodeSelectDB}
	payload = append(payload, encodeLength(index)...)
	return payload
}

func resizeDB(mainSize, expirySize uint64) []byte {
	payload := []byte{opcodeResizeDB}
	payload = append(payload, encodeLength(mainSize)...)
	payload = append(payload, encodeLength(expirySize)...)
	return payload
}

func stringEntry(key, value []byte) []byte {
	payload := []byte{valueTypeString}
	payload = append(payload, key...)
	payload = append(payload, value...)
	return payload
}

func expiringStringEntrySeconds(expiresAtSeconds uint32, key, value []byte) []byte {
	payload := []byte{opcodeExpireTimeSec}
	raw := make([]byte, 4)
	binary.LittleEndian.PutUint32(raw, expiresAtSeconds)
	payload = append(payload, raw...)
	payload = append(payload, stringEntry(key, value)...)
	return payload
}

func expiringStringEntryMillis(expiresAtMillis uint64, key, value []byte) []byte {
	payload := []byte{opcodeExpireTimeMS}
	raw := make([]byte, 8)
	binary.LittleEndian.PutUint64(raw, expiresAtMillis)
	payload = append(payload, raw...)
	payload = append(payload, stringEntry(key, value)...)
	return payload
}

func rawString(value []byte) []byte {
	payload := encodeLength(uint64(len(value)))
	payload = append(payload, value...)
	return payload
}

func int32String(value int32) []byte {
	payload := []byte{0xC2}
	raw := make([]byte, 4)
	binary.LittleEndian.PutUint32(raw, uint32(value))
	payload = append(payload, raw...)
	return payload
}

func lzfString(value []byte) []byte {
	compressed := append([]byte{byte(len(value) - 1)}, value...)
	payload := []byte{0xC3}
	payload = append(payload, encodeLength(uint64(len(compressed)))...)
	payload = append(payload, encodeLength(uint64(len(value)))...)
	payload = append(payload, compressed...)
	return payload
}

func encodeLength(length uint64) []byte {
	switch {
	case length < 1<<6:
		return []byte{byte(length)}
	case length < 1<<14:
		return []byte{byte((length>>8)&0x3F) | 0x40, byte(length)}
	default:
		raw := make([]byte, 5)
		raw[0] = 0x80
		binary.BigEndian.PutUint32(raw[1:], uint32(length))
		return raw
	}
}

func TestLoadReaderWithRealisticAbsoluteExpiry(t *testing.T) {
	store := storage.NewStore()
	now := time.Now().UnixMilli()
	payload := buildRDBPayload(
		selectDB(0),
		expiringStringEntryMillis(uint64(now+5000), rawString([]byte("ttl")), rawString([]byte("fresh"))),
	)

	stats, err := loadReaderAt(bytes.NewReader(payload), store, func() int64 { return now })
	if err != nil {
		t.Fatalf("loadReaderAt() error = %v", err)
	}
	if stats.LoadedKeys != 1 {
		t.Fatalf("LoadedKeys = %d, want 1", stats.LoadedKeys)
	}
	assertStoredString(t, store, "ttl", "fresh")
}
