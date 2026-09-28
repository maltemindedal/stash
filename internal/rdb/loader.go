package rdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/maltemindedal/stash/internal/storage"
)

const (
	fileHeader = "REDIS0011"

	opcodeAux           = 0xFA
	opcodeResizeDB      = 0xFB
	opcodeExpireTimeMS  = 0xFC
	opcodeExpireTimeSec = 0xFD
	opcodeSelectDB      = 0xFE
	opcodeEOF           = 0xFF

	valueTypeString = 0x00

	lengthEncoding6Bit  = 0x00
	lengthEncoding14Bit = 0x01
	lengthEncoding32Bit = 0x02
	lengthEncodingEnc   = 0x03

	stringEncodingInt8  = 0x00
	stringEncodingInt16 = 0x01
	stringEncodingInt32 = 0x02
	stringEncodingLZF   = 0x03
)

var (
	// ErrInvalidHeader reports that the RDB file does not start with REDIS0011.
	ErrInvalidHeader = errors.New("rdb: invalid file header")
	// ErrUnsupportedDB reports that the loader encountered a non-zero database selector.
	ErrUnsupportedDB = errors.New("rdb: unsupported database selector")
	// ErrUnsupportedValueType reports that the loader encountered a non-string value type.
	ErrUnsupportedValueType = errors.New("rdb: unsupported value type")
	// ErrUnsupportedOpcode reports that the loader encountered an opcode it does not implement.
	ErrUnsupportedOpcode = errors.New("rdb: unsupported opcode")
)

// Stats summarizes the results of loading an RDB snapshot.
type Stats struct {
	LoadedKeys         int
	SkippedExpiredKeys int
}

// LoadFile opens an RDB file from disk and loads its supported contents into the store.
func LoadFile(path string, store *storage.Store) (stats Stats, err error) {
	file, err := os.Open(path)
	if err != nil {
		return Stats{}, fmt.Errorf("rdb: open %q: %w", path, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			wrapped := fmt.Errorf("rdb: close %q: %w", path, closeErr)
			if err == nil {
				err = wrapped
				return
			}

			err = errors.Join(err, wrapped)
		}
	}()

	stats, err = LoadReader(file, store)
	if err != nil {
		return Stats{}, fmt.Errorf("rdb: load %q: %w", path, err)
	}

	return stats, nil
}

// LoadReader loads an RDB payload from the provided reader into the store.
func LoadReader(reader io.Reader, store *storage.Store) (Stats, error) {
	return loadReaderAt(reader, store, func() int64 { return time.Now().UnixMilli() })
}

type loader struct {
	reader   *bufio.Reader
	store    *storage.Store
	nowMilli func() int64
	stats    Stats
	seenEOF  bool
}

func loadReaderAt(reader io.Reader, store *storage.Store, nowMilli func() int64) (Stats, error) {
	if store == nil {
		return Stats{}, errors.New("rdb: nil store")
	}
	if reader == nil {
		return Stats{}, errors.New("rdb: nil reader")
	}

	loader := &loader{
		reader:   bufio.NewReader(reader),
		store:    store,
		nowMilli: nowMilli,
	}

	if err := loader.load(); err != nil {
		return Stats{}, err
	}

	return loader.stats, nil
}

// rdbMaxSupportedVersion is the highest RDB format version this loader accepts.
// Files declaring an older version (REDIS0001..REDIS0011) are still readable, so
// the header is checked by prefix and version rather than an exact string match.
const rdbMaxSupportedVersion = 11

func (l *loader) load() error {
	header := make([]byte, len(fileHeader))
	if _, err := io.ReadFull(l.reader, header); err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	if err := validateHeader(header); err != nil {
		return err
	}

	for !l.seenEOF {
		marker, err := l.readByte()
		if err != nil {
			return fmt.Errorf("read marker: %w", err)
		}

		if err := l.processMarker(marker); err != nil {
			return err
		}
	}

	return nil
}

func validateHeader(header []byte) error {
	if len(header) != len(fileHeader) || string(header[:5]) != "REDIS" {
		return fmt.Errorf("%w: got %q", ErrInvalidHeader, string(header))
	}
	version, err := strconv.Atoi(string(header[5:]))
	if err != nil || version < 1 || version > rdbMaxSupportedVersion {
		return fmt.Errorf("%w: unsupported version in %q", ErrInvalidHeader, string(header))
	}

	return nil
}

func (l *loader) processMarker(marker byte) error {
	switch marker {
	case opcodeAux:
		return l.skipAuxField()
	case opcodeResizeDB:
		return l.skipResizeDBMetadata()
	case opcodeSelectDB:
		return l.readDatabaseSelection()
	case opcodeExpireTimeSec:
		expiresAtSeconds, err := l.readUint32LE()
		if err != nil {
			return fmt.Errorf("read EXPIRETIME: %w", err)
		}

		return l.readExpiringEntry("EXPIRETIME", int64(expiresAtSeconds)*1000)
	case opcodeExpireTimeMS:
		expiresAtMillis, err := l.readUint64LE()
		if err != nil {
			return fmt.Errorf("read EXPIRETIMEMS: %w", err)
		}

		validatedMillis, err := validatedExpiryMillis(expiresAtMillis)
		if err != nil {
			return err
		}

		return l.readExpiringEntry("EXPIRETIMEMS", validatedMillis)
	case opcodeEOF:
		return l.consumeEOF()
	default:
		return l.readValueMarker(marker)
	}
}

func (l *loader) skipAuxField() error {
	if _, err := l.readString(); err != nil {
		return fmt.Errorf("read AUX key: %w", err)
	}
	if _, err := l.readString(); err != nil {
		return fmt.Errorf("read AUX value: %w", err)
	}

	return nil
}

func (l *loader) skipResizeDBMetadata() error {
	if _, _, err := l.readLength(); err != nil {
		return fmt.Errorf("read RESIZEDB main hash size: %w", err)
	}
	if _, _, err := l.readLength(); err != nil {
		return fmt.Errorf("read RESIZEDB expiry hash size: %w", err)
	}

	return nil
}

func (l *loader) readDatabaseSelection() error {
	dbIndex, _, err := l.readLength()
	if err != nil {
		return fmt.Errorf("read SELECTDB: %w", err)
	}
	if dbIndex != 0 {
		return fmt.Errorf("%w: %d", ErrUnsupportedDB, dbIndex)
	}

	return nil
}

func (l *loader) readExpiringEntry(opcodeName string, expiresAt int64) error {
	valueType, err := l.readByte()
	if err != nil {
		return fmt.Errorf("read value type after %s: %w", opcodeName, err)
	}

	return l.readEntry(valueType, expiresAt)
}

func (l *loader) consumeEOF() error {
	if err := l.consumeChecksum(); err != nil {
		return err
	}

	l.seenEOF = true
	return nil
}

func (l *loader) readValueMarker(marker byte) error {
	if !isValueType(marker) {
		return fmt.Errorf("%w: 0x%X", ErrUnsupportedOpcode, marker)
	}

	return l.readEntry(marker, 0)
}

func validatedExpiryMillis(expiresAtMillis uint64) (int64, error) {
	if expiresAtMillis > ^uint64(0)>>1 {
		return 0, fmt.Errorf("rdb: expiry milliseconds overflow: %d", expiresAtMillis)
	}

	return int64(expiresAtMillis), nil
}

func (l *loader) readEntry(valueType byte, expiresAt int64) error {
	if valueType != valueTypeString {
		return fmt.Errorf("%w: %d", ErrUnsupportedValueType, valueType)
	}

	key, err := l.readString()
	if err != nil {
		return fmt.Errorf("read key: %w", err)
	}
	value, err := l.readString()
	if err != nil {
		return fmt.Errorf("read string value: %w", err)
	}

	if expiresAt > 0 && expiresAt <= l.nowMilli() {
		l.stats.SkippedExpiredKeys++
		return nil
	}

	// Startup loading runs before maxmemory is configured, so Set cannot evict
	// or reject today. It is still propagated rather than dropped: the loader
	// rejects what it cannot fully apply instead of loading partially.
	if _, err := l.store.Set(string(key), value, expiresAt); err != nil {
		return fmt.Errorf("store key %q: %w", key, err)
	}
	l.stats.LoadedKeys++
	return nil
}

func (l *loader) readByte() (byte, error) {
	value, err := l.reader.ReadByte()
	if err != nil {
		return 0, err
	}

	return value, nil
}

func (l *loader) readLength() (uint64, bool, error) {
	first, err := l.readByte()
	if err != nil {
		return 0, false, err
	}

	switch first >> 6 {
	case lengthEncoding6Bit:
		return uint64(first & 0x3F), false, nil
	case lengthEncoding14Bit:
		second, err := l.readByte()
		if err != nil {
			return 0, false, err
		}
		return (uint64(first&0x3F) << 8) | uint64(second), false, nil
	case lengthEncoding32Bit:
		raw := make([]byte, 4)
		if _, err := io.ReadFull(l.reader, raw); err != nil {
			return 0, false, err
		}
		return uint64(binary.BigEndian.Uint32(raw)), false, nil
	case lengthEncodingEnc:
		return uint64(first & 0x3F), true, nil
	default:
		return 0, false, fmt.Errorf("rdb: invalid length prefix %d", first)
	}
}

func (l *loader) readString() ([]byte, error) {
	length, encoded, err := l.readLength()
	if err != nil {
		return nil, err
	}
	if !encoded {
		return l.readBytes(length)
	}

	switch byte(length) {
	case stringEncodingInt8:
		value, err := l.readByte()
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int8(value)), 10)), nil
	case stringEncodingInt16:
		value, err := l.readUint16LE()
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int16(value)), 10)), nil
	case stringEncodingInt32:
		value, err := l.readUint32LE()
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(value)), 10)), nil
	case stringEncodingLZF:
		compressedLength, compressedEncoded, err := l.readLength()
		if err != nil {
			return nil, fmt.Errorf("read LZF compressed length: %w", err)
		}
		if compressedEncoded {
			return nil, errors.New("rdb: invalid encoded compressed length")
		}

		uncompressedLength, uncompressedEncoded, err := l.readLength()
		if err != nil {
			return nil, fmt.Errorf("read LZF uncompressed length: %w", err)
		}
		if uncompressedEncoded {
			return nil, errors.New("rdb: invalid encoded uncompressed length")
		}
		if uncompressedLength > maxRDBObjectLength {
			return nil, fmt.Errorf("rdb: LZF uncompressed length %d exceeds %d byte limit", uncompressedLength, maxRDBObjectLength)
		}

		compressed, err := l.readBytes(compressedLength)
		if err != nil {
			return nil, fmt.Errorf("read LZF payload: %w", err)
		}

		return decompressLZF(compressed, int(uncompressedLength))
	default:
		return nil, fmt.Errorf("rdb: unsupported string encoding %d", length)
	}
}

// maxRDBObjectLength bounds a single declared object/allocation length in an RDB
// stream. The loader also runs on data received from a replication master, so a
// forged length field must not be able to pre-allocate an unbounded buffer and
// OOM the process before the short read is detected. It mirrors the protocol
// layer's 512 MiB bulk-string limit.
const maxRDBObjectLength = 512 * 1024 * 1024

// rdbFirstAllocation is the most readBytes sizes a buffer for before any of the
// object's bytes have been read. A longer object grows the buffer as bytes
// arrive, so a declared length that the input never backs cannot reserve up to
// maxRDBObjectLength of memory.
const rdbFirstAllocation = 1024 * 1024

func (l *loader) readBytes(length uint64) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}
	if length > maxRDBObjectLength {
		return nil, fmt.Errorf("rdb: declared object length %d exceeds %d byte limit", length, maxRDBObjectLength)
	}

	buffer := make([]byte, min(length, rdbFirstAllocation))
	if _, err := io.ReadFull(l.reader, buffer); err != nil {
		return nil, err
	}
	for uint64(len(buffer)) < length {
		grown := make([]byte, min(length, 2*uint64(len(buffer))))
		copy(grown, buffer)
		if _, err := io.ReadFull(l.reader, grown[len(buffer):]); err != nil {
			if errors.Is(err, io.EOF) {
				// Some of the object was already read, so it was cut short;
				// io.EOF would read as a clean end of input.
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
		buffer = grown
	}

	return buffer, nil
}

func (l *loader) readUint16LE() (uint16, error) {
	raw := make([]byte, 2)
	if _, err := io.ReadFull(l.reader, raw); err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint16(raw), nil
}

func (l *loader) readUint32LE() (uint32, error) {
	raw := make([]byte, 4)
	if _, err := io.ReadFull(l.reader, raw); err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint32(raw), nil
}

func (l *loader) readUint64LE() (uint64, error) {
	raw := make([]byte, 8)
	if _, err := io.ReadFull(l.reader, raw); err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(raw), nil
}

func (l *loader) consumeChecksum() error {
	// Count what remains instead of buffering it: only 0 or 8 bytes are valid, and
	// junk of any length must be reported without being held in memory.
	remaining, err := io.Copy(io.Discard, l.reader)
	if err != nil {
		return fmt.Errorf("read trailing checksum: %w", err)
	}

	if remaining != 0 && remaining != 8 {
		return fmt.Errorf("rdb: invalid trailing checksum length %d", remaining)
	}

	return nil
}

func isValueType(marker byte) bool {
	switch marker {
	case 0, 1, 2, 3, 4, 9, 10, 11, 12, 13, 14:
		return true
	default:
		return false
	}
}

func decompressLZF(input []byte, outputLength int) ([]byte, error) {
	output := make([]byte, outputLength)
	ip := 0
	op := 0

	for ip < len(input) {
		control := int(input[ip])
		ip++

		if control < 32 {
			nextIP, nextOP, err := decodeLZFLiteral(input, output, ip, op, control)
			if err != nil {
				return nil, err
			}

			ip = nextIP
			op = nextOP
			continue
		}

		nextIP, nextOP, err := decodeLZFMatch(input, output, ip, op, control)
		if err != nil {
			return nil, err
		}

		ip = nextIP
		op = nextOP
	}

	if op != len(output) {
		return nil, fmt.Errorf("rdb: LZF output length mismatch got %d want %d", op, len(output))
	}

	return output, nil
}

func decodeLZFLiteral(input []byte, output []byte, inputPos int, outputPos int, control int) (int, int, error) {
	literalLength := control + 1
	if inputPos+literalLength > len(input) {
		return 0, 0, errors.New("rdb: truncated LZF literal")
	}
	if outputPos+literalLength > len(output) {
		return 0, 0, errors.New("rdb: LZF literal exceeds output length")
	}

	copy(output[outputPos:outputPos+literalLength], input[inputPos:inputPos+literalLength])
	return inputPos + literalLength, outputPos + literalLength, nil
}

func decodeLZFMatch(input []byte, output []byte, inputPos int, outputPos int, control int) (int, int, error) {
	matchLength := control >> 5
	if matchLength == 7 {
		if inputPos >= len(input) {
			return 0, 0, errors.New("rdb: truncated LZF match length")
		}

		matchLength += int(input[inputPos])
		inputPos++
	}
	if inputPos >= len(input) {
		return 0, 0, errors.New("rdb: truncated LZF match offset")
	}

	reference := outputPos - ((control & 0x1F) << 8) - int(input[inputPos]) - 1
	inputPos++
	matchLength += 2

	if reference < 0 {
		return 0, 0, errors.New("rdb: invalid LZF back-reference")
	}
	if outputPos+matchLength > len(output) {
		return 0, 0, errors.New("rdb: LZF match exceeds output length")
	}

	for i := 0; i < matchLength; i++ {
		output[outputPos] = output[reference+i]
		outputPos++
	}

	return inputPos, outputPos, nil
}
