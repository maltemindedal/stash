package storage

import (
	"bytes"
	"hash/fnv"
	"math"
	"math/bits"
)

// HyperLogLog values are stored as string values with a fixed dense layout:
// a 16-byte header (magic + encoding version) followed by one byte per
// register. The representation never grows after creation, which keeps every
// HyperLogLog key at a fixed bounded footprint.
const (
	hllIndexBits     = 14
	hllRegisterCount = 1 << hllIndexBits
	// hllHeaderSize reserves 16 bytes: 4 magic bytes, 1 encoding version
	// byte, and 11 reserved zero bytes for future header fields.
	hllHeaderSize    = 16
	hllValueSize     = hllHeaderSize + hllRegisterCount
	hllDenseEncoding = 1
	hllMaxRank       = 64 - hllIndexBits + 1
)

// hllMagic deliberately differs from Redis's "HYLL" magic: the register
// layout is not interoperable with Redis HyperLogLog strings, so a distinct
// magic makes values from either side fail closed instead of parsing as
// corrupt estimates.
var hllMagic = []byte{'R', 'H', 'L', 'L'}

// hllRankReciprocal maps a register rank to 2^-rank for the estimator sum.
var hllRankReciprocal = func() [hllMaxRank + 1]float64 {
	var table [hllMaxRank + 1]float64
	for rank := range table {
		table[rank] = math.Ldexp(1, -rank)
	}
	return table
}()

// PFAdd registers elements into a HyperLogLog value and reports whether the
// cardinality estimate changed. When maxmemory is configured it first frees
// space and reports the keys evicted to make room; otherwise it evicts nothing.
func (s *Store) PFAdd(key string, elements [][]byte) (int64, []string, error) {
	return writeKey(s, key, func(w keyWrite) (int64, []string, error) {
		if w.current == nil {
			payload := newHyperLogLogPayload()
			updateHyperLogLogRegisters(payload[hllHeaderSize:], elements)
			newValue := newOwnedStringValue(payload, 0)
			newValue.touch(w.now)
			evicted, err := w.commit(newValue)
			if err != nil {
				return 0, nil, err
			}
			return 1, evicted, nil
		}

		data, err := w.current.StringValue()
		if err != nil {
			return 0, nil, err
		}
		registers, err := hyperLogLogRegisters(data)
		if err != nil {
			return 0, nil, err
		}
		// Registers are updated where they lie, even while accounting: the
		// layout is fixed at creation and hyperLogLogRegisters rejects any
		// payload of another length, so this write cannot change the key's size
		// and has nothing for commit to weigh.
		changed := updateHyperLogLogRegisters(registers, elements)
		w.current.touch(w.now)
		return changed, nil, nil
	})
}

// PFCount returns the approximate cardinality of one HyperLogLog key or of
// the union of multiple HyperLogLog keys. Missing keys count as empty.
func (s *Store) PFCount(keys []string) (int64, error) {
	merged := make([]byte, hllRegisterCount)
	for _, key := range keys {
		if err := s.mergeHyperLogLogRegisters(key, merged); err != nil {
			return 0, err
		}
	}

	return hyperLogLogEstimate(merged), nil
}

// mergeHyperLogLogRegisters folds the key's registers into merged by
// register-wise maximum without copying the stored value.
func (s *Store) mergeHyperLogLogRegisters(key string, merged []byte) error {
	_, err := s.withStringValue(key, func(data []byte) error {
		registers, err := hyperLogLogRegisters(data)
		if err != nil {
			return err
		}
		for i, rank := range registers {
			if merged[i] < rank {
				merged[i] = rank
			}
		}
		return nil
	})
	return err
}

func newHyperLogLogPayload() []byte {
	payload := make([]byte, hllValueSize)
	copy(payload, hllMagic)
	payload[len(hllMagic)] = hllDenseEncoding
	return payload
}

// hyperLogLogRegisters validates the header and register ranges of a stored
// string value. Range validation matters because SET and SETBIT can write
// arbitrary bytes into a string key: a register above hllMaxRank would
// silently corrupt the estimator instead of failing.
func hyperLogLogRegisters(data []byte) ([]byte, error) {
	if len(data) != hllValueSize || !bytes.Equal(data[:len(hllMagic)], hllMagic) || data[len(hllMagic)] != hllDenseEncoding {
		return nil, ErrNotHyperLogLog
	}
	registers := data[hllHeaderSize:]
	for _, rank := range registers {
		if rank > hllMaxRank {
			return nil, ErrNotHyperLogLog
		}
	}
	return registers, nil
}

func updateHyperLogLogRegisters(registers []byte, elements [][]byte) int64 {
	changed := int64(0)
	for _, element := range elements {
		index, rank := hyperLogLogPosition(element)
		if registers[index] < rank {
			registers[index] = rank
			changed = 1
		}
	}
	return changed
}

// hyperLogLogPosition hashes an element into a register index taken from the
// top hllIndexBits bits and a rank derived from the remaining bits. The hash
// must stay deterministic across process restarts because stored registers
// are replayed through AOF and RDB persistence. FNV-1a alone distributes
// similar short keys poorly across the high index bits, so the sum is passed
// through the MurmurHash3 64-bit finalizer for avalanche.
func hyperLogLogPosition(element []byte) (int, byte) {
	hasher := fnv.New64a()
	_, _ = hasher.Write(element)
	sum := mix64(hasher.Sum64())

	index := int(sum >> (64 - hllIndexBits))
	rank := min(bits.LeadingZeros64(sum<<hllIndexBits)+1, hllMaxRank)
	return index, byte(rank)
}

// mix64 is the MurmurHash3 64-bit finalizer.
func mix64(sum uint64) uint64 {
	sum ^= sum >> 33
	sum *= 0xff51afd7ed558ccd
	sum ^= sum >> 33
	sum *= 0xc4ceb9fe1a85ec53
	sum ^= sum >> 33
	return sum
}

// hyperLogLogEstimate applies the standard HyperLogLog estimator with the
// linear-counting correction for small cardinalities. Registers must already
// be range-validated so ranks index hllRankReciprocal safely.
func hyperLogLogEstimate(registers []byte) int64 {
	m := float64(hllRegisterCount)
	alpha := 0.7213 / (1 + 1.079/m)

	sum := 0.0
	zeros := 0
	for _, rank := range registers {
		sum += hllRankReciprocal[rank]
		if rank == 0 {
			zeros++
		}
	}

	estimate := alpha * m * m / sum
	if estimate <= 2.5*m && zeros > 0 {
		estimate = m * math.Log(m/float64(zeros))
	}
	if estimate >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(math.Round(estimate))
}
