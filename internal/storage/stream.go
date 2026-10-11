package storage

import (
	"sort"
	"strconv"
	"strings"
)

type streamID struct {
	milliseconds int64
	sequence     int64
}

type streamRecord struct {
	id     streamID
	idText string
	values [][]byte
}

// StreamValue holds the entries of a single stream key in ascending ID order,
// along with the highest ID admitted so far so XADD can reject non-monotonic
// explicit IDs.
type StreamValue struct {
	entries    []streamRecord
	lastID     streamID
	hasEntries bool
}

func newStream() *StreamValue {
	return &StreamValue{}
}

// containerCopy returns a copy of s that XADD can append to while s stays
// stored: a new slice of the same records. A record is never edited once it is
// added, so the copies share them.
func (s *StreamValue) containerCopy() *StreamValue {
	if s == nil {
		return nil
	}
	return &StreamValue{
		entries:    append([]streamRecord(nil), s.entries...),
		lastID:     s.lastID,
		hasEntries: s.hasEntries,
	}
}

// ValidateXAddID reports whether raw is a syntactically valid XADD ID.
func ValidateXAddID(raw string) error {
	if raw == "*" {
		return nil
	}

	_, err := parseStreamAddID(raw)
	return err
}

// ValidateXReadID reports whether raw is a syntactically valid XREAD ID.
func ValidateXReadID(raw string) error {
	if raw == "$" {
		return nil
	}
	if _, _, ok := strings.Cut(raw, "-"); !ok {
		_, err := parseNonNegativeStreamInt(raw)
		return err
	}

	_, err := parseStreamAddID(raw)
	return err
}

func (s *StreamValue) add(rawID string, values [][]byte, nowMillis int64) (string, error) {
	var (
		id  streamID
		err error
	)

	if rawID == "*" {
		id = s.nextAutoID(nowMillis)
	} else {
		id, err = parseStreamAddID(rawID)
		if err != nil {
			return "", err
		}
		if s.hasEntries && compareStreamIDs(id, s.lastID) <= 0 {
			return "", ErrStreamIDTooSmall
		}
	}

	idText := id.String()
	s.entries = append(s.entries, streamRecord{id: id, idText: idText, values: cloneList(values)})
	s.lastID = id
	s.hasEntries = true

	return idText, nil
}

func (s *StreamValue) entriesAfter(rawID string) ([]StreamEntry, error) {
	if len(s.entries) == 0 {
		return []StreamEntry{}, nil
	}

	after, err := parseStreamReadID(rawID, s.lastID)
	if err != nil {
		return nil, err
	}

	start := sort.Search(len(s.entries), func(i int) bool {
		return compareStreamIDs(s.entries[i].id, after) > 0
	})
	if start == len(s.entries) {
		return []StreamEntry{}, nil
	}

	entries := make([]StreamEntry, len(s.entries)-start)
	for i, record := range s.entries[start:] {
		entries[i] = StreamEntry{ID: record.idText, Values: cloneList(record.values)}
	}

	return entries, nil
}

func (s *StreamValue) nextAutoID(nowMillis int64) streamID {
	if !s.hasEntries {
		return streamID{milliseconds: nowMillis, sequence: 0}
	}

	if nowMillis < s.lastID.milliseconds {
		return streamID{milliseconds: s.lastID.milliseconds, sequence: s.lastID.sequence + 1}
	}
	if nowMillis == s.lastID.milliseconds {
		return streamID{milliseconds: nowMillis, sequence: s.lastID.sequence + 1}
	}

	return streamID{milliseconds: nowMillis, sequence: 0}
}

// String renders the ID in the Redis <milliseconds>-<sequence> wire form.
func (id streamID) String() string {
	return strconv.FormatInt(id.milliseconds, 10) + "-" + strconv.FormatInt(id.sequence, 10)
}

func compareStreamIDs(left, right streamID) int {
	if left.milliseconds < right.milliseconds {
		return -1
	}
	if left.milliseconds > right.milliseconds {
		return 1
	}
	if left.sequence < right.sequence {
		return -1
	}
	if left.sequence > right.sequence {
		return 1
	}

	return 0
}

func parseStreamAddID(raw string) (streamID, error) {
	millisecondsPart, sequencePart, ok := strings.Cut(raw, "-")
	if !ok || millisecondsPart == "" || sequencePart == "" || strings.Contains(sequencePart, "-") {
		return streamID{}, ErrInvalidStreamID
	}

	milliseconds, err := parseNonNegativeStreamInt(millisecondsPart)
	if err != nil {
		return streamID{}, err
	}
	sequence, err := parseNonNegativeStreamInt(sequencePart)
	if err != nil {
		return streamID{}, err
	}

	return streamID{milliseconds: milliseconds, sequence: sequence}, nil
}

func parseStreamReadID(raw string, latest streamID) (streamID, error) {
	if raw == "$" {
		return latest, nil
	}
	if _, _, ok := strings.Cut(raw, "-"); !ok {
		milliseconds, err := parseNonNegativeStreamInt(raw)
		if err != nil {
			return streamID{}, err
		}
		return streamID{milliseconds: milliseconds, sequence: 0}, nil
	}

	return parseStreamAddID(raw)
}

func parseNonNegativeStreamInt(raw string) (int64, error) {
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0, ErrInvalidStreamID
	}

	return value, nil
}
