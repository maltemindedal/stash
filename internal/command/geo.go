package command

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

// Geospatial commands encode longitude/latitude pairs into 52-bit interleaved
// geohash scores stored in regular sorted sets, matching the Redis layout.
const (
	geoLongitudeMin = -180.0
	geoLongitudeMax = 180.0
	geoLatitudeMin  = -85.05112878
	geoLatitudeMax  = 85.05112878

	// geoStep is the number of bits used per coordinate.
	geoStep = 26

	geoEarthRadiusMeters = 6372797.560856

	geoMetersPerKilometer = 1000.0
	geoMetersPerMile      = 1609.34
	geoMetersPerFoot      = 0.3048
)

func (e *Executor) handleGeoAdd(ctx context.Context, request *Request) (protocol.Value, error) {
	entries := make([]storage.ZSetEntry, 0, (len(request.Args)-1)/3)
	for i := 1; i < len(request.Args); i += 3 {
		longitude, latitude, err := parseGeoCoordinates(request.Args[i], request.Args[i+1])
		if err != nil {
			return nil, err
		}

		entries = append(entries, storage.ZSetEntry{
			Member: request.Args[i+2],
			Score:  float64(geohashEncode(longitude, latitude)),
		})
	}

	key := string(request.Args[0])
	added, evicted, err := e.store.ZAdd(key, entries)
	if err != nil {
		return nil, storageCommandError(err)
	}

	e.recordWriteEffects(request, key, evicted)
	return protocol.Integer{Value: added}, nil
}

func (e *Executor) handleGeoDist(_ context.Context, request *Request) (protocol.Value, error) {
	unitMeters := 1.0
	if len(request.Args) == 4 {
		var err error
		unitMeters, err = parseGeoUnitArgument(request.Args[3])
		if err != nil {
			return nil, err
		}
	}

	key := string(request.Args[0])
	scores, found, err := e.store.ZScores(key, [][]byte{request.Args[1], request.Args[2]})
	if err != nil {
		return nil, storageCommandError(err)
	}
	if !found[0] || !found[1] {
		return protocol.BulkString{Null: true}, nil
	}

	firstLongitude, firstLatitude := geohashDecode(geohashBits(scores[0]))
	secondLongitude, secondLatitude := geohashDecode(geohashBits(scores[1]))
	distance := geoDistanceMeters(firstLongitude, firstLatitude, secondLongitude, secondLatitude) / unitMeters

	return protocol.TextBulkString{Value: strconv.FormatFloat(distance, 'f', 4, 64)}, nil
}

func (e *Executor) handleGeoRadius(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 5 {
		return nil, wrongNumberOfArgumentsError("GEORADIUS")
	}
	if len(request.Args) > 5 {
		return nil, ErrSyntaxError()
	}

	longitude, latitude, err := parseGeoCoordinates(request.Args[1], request.Args[2])
	if err != nil {
		return nil, err
	}
	radius, err := parseGeoRadiusArgument(request.Args[3])
	if err != nil {
		return nil, err
	}
	unitMeters, err := parseGeoUnitArgument(request.Args[4])
	if err != nil {
		return nil, err
	}
	radiusMeters := radius * unitMeters

	key := string(request.Args[0])
	entries, err := e.store.ZRangeByScores(key, geoRadiusScoreRanges(longitude, latitude, radiusMeters)...)
	if err != nil {
		if errors.Is(err, storage.ErrWrongType) {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}

	elements := make([]protocol.Value, 0, len(entries))
	for _, entry := range entries {
		memberLongitude, memberLatitude := geohashDecode(geohashBits(entry.Score))
		if geoDistanceMeters(longitude, latitude, memberLongitude, memberLatitude) <= radiusMeters {
			elements = append(elements, protocol.TextBulkString{Value: entry.Member})
		}
	}

	return protocol.Array{Elements: elements}, nil
}

// geoRadiusScoreRanges returns the disjoint, ascending score ranges GEORADIUS
// scans instead of the full sorted set: the geohash cell containing the
// center at the deepest bit depth whose cell size covers the radius bounding
// box, plus its eight neighbors. Two guard ranges bracket the cells for the
// scores that fall outside every cell range: plain ZADD can write arbitrary
// scores (decoded as the corner cell of the coordinate space), and GEOADD at
// exactly the maximum longitude or latitude encodes above 52 bits. Scanning
// the ranges in order yields members in sorted-set order, so results match a
// full scan exactly.
func geoRadiusScoreRanges(longitude, latitude, radiusMeters float64) []storage.ScoreRange {
	deltaLatitude, deltaLongitude := geoBoundingDeltas(latitude, radiusMeters)
	step := uint(geoStep)
	for step > 1 && !geoCellCovers(step, deltaLatitude, deltaLongitude) {
		step--
	}

	cellRanges := geoNeighborCellRanges(longitude, latitude, step)

	ranges := make([]storage.ScoreRange, 0, len(cellRanges)+2)
	ranges = append(ranges, storage.ScoreRange{Min: math.Inf(-1), Max: 0, MaxExclusive: true})
	ranges = append(ranges, cellRanges...)
	ranges = append(ranges, storage.ScoreRange{Min: float64(uint64(1) << (2 * geoStep)), Max: math.Inf(1)})
	return ranges
}

// geoBoundingDeltas returns the half-height and half-width in degrees of the
// smallest latitude/longitude box containing every point within radiusMeters
// of a center at the given latitude. A half-width of 180 means the box wraps
// the full longitude range.
func geoBoundingDeltas(latitude, radiusMeters float64) (deltaLatitude, deltaLongitude float64) {
	radiusRadians := radiusMeters / geoEarthRadiusMeters
	deltaLatitude = radiansToDegrees(radiusRadians)
	if math.Abs(latitude)+deltaLatitude >= 90 {
		return deltaLatitude, 180
	}

	// The longitude extent is widest at the box's poleward edge, where the
	// latitude circle is smallest (cos is smallest), not at the center latitude.
	// Evaluating at |latitude|+deltaLatitude matches Redis' geohashBoundingBox
	// and avoids an under-sized box that could drop in-radius members.
	outerLatitude := math.Abs(latitude) + deltaLatitude
	ratio := math.Sin(radiusRadians) / math.Cos(degreesToRadians(outerLatitude))
	if ratio >= 1 {
		// The box wraps around the pole in longitude; cover the full range.
		return deltaLatitude, 180
	}
	deltaLongitude = radiansToDegrees(math.Asin(ratio))
	return deltaLatitude, deltaLongitude
}

// geoCellCovers reports whether a single geohash cell at the given step is at
// least as large as the bounding-box half-deltas, which guarantees the cell
// containing the center plus its eight neighbors cover the whole search box.
func geoCellCovers(step uint, deltaLatitude, deltaLongitude float64) bool {
	cells := float64(uint64(1) << step)
	cellHeight := (geoLatitudeMax - geoLatitudeMin) / cells
	cellWidth := (geoLongitudeMax - geoLongitudeMin) / cells
	return cellHeight >= deltaLatitude && cellWidth >= deltaLongitude
}

// geoNeighborCellRanges returns the score ranges of the geohash cell
// containing the center and its eight neighbors at the given step, wrapping
// longitude across the antimeridian and dropping latitude neighbors beyond
// the poles. The ranges are sorted ascending and adjacent or duplicate ranges
// are merged so each is scanned once.
func geoNeighborCellRanges(longitude, latitude float64, step uint) []storage.ScoreRange {
	centerLatCell, centerLonCell := geoCellCoords(longitude, latitude, step)
	cells := int64(1) << step
	shift := 2 * (geoStep - step)

	ranges := make([]storage.ScoreRange, 0, 9)
	for deltaLat := int64(-1); deltaLat <= 1; deltaLat++ {
		latCell := int64(centerLatCell) + deltaLat
		if latCell < 0 || latCell >= cells {
			continue
		}
		for deltaLon := int64(-1); deltaLon <= 1; deltaLon++ {
			lonCell := (int64(centerLonCell) + deltaLon + cells) % cells
			minBits := interleave64(uint32(latCell), uint32(lonCell)) << shift
			ranges = append(ranges, storage.ScoreRange{
				Min:          float64(minBits),
				Max:          float64(minBits + uint64(1)<<shift),
				MaxExclusive: true,
			})
		}
	}

	sort.Slice(ranges, func(i, j int) bool { return ranges[i].Min < ranges[j].Min })

	merged := make([]storage.ScoreRange, 0, len(ranges))
	merged = append(merged, ranges[0])
	for _, scoreRange := range ranges[1:] {
		last := &merged[len(merged)-1]
		if scoreRange.Min <= last.Max {
			last.Max = math.Max(last.Max, scoreRange.Max)
			continue
		}
		merged = append(merged, scoreRange)
	}
	return merged
}

// geoCellCoords returns the latitude and longitude cell indexes at the given
// step of the geohash cell containing the position, derived from the same
// encoding that produces stored scores so cell indexes are score prefixes by
// construction. The exact upper coordinate boundary encodes one past the last
// cell and is clamped into it.
func geoCellCoords(longitude, latitude float64, step uint) (latCell, lonCell uint64) {
	latitudeBits, longitudeBits := deinterleave64(geohashEncode(longitude, latitude))
	cells := uint64(1) << step

	latCell = uint64(latitudeBits) >> (geoStep - step)
	if latCell >= cells {
		latCell = cells - 1
	}
	lonCell = uint64(longitudeBits) >> (geoStep - step)
	if lonCell >= cells {
		lonCell = cells - 1
	}
	return latCell, lonCell
}

func parseGeoCoordinates(rawLongitude, rawLatitude []byte) (float64, float64, error) {
	longitude, err := parseFloatArgument(rawLongitude)
	if err != nil {
		return 0, 0, err
	}
	latitude, err := parseFloatArgument(rawLatitude)
	if err != nil {
		return 0, 0, err
	}
	if longitude < geoLongitudeMin || longitude > geoLongitudeMax ||
		latitude < geoLatitudeMin || latitude > geoLatitudeMax {
		return 0, 0, newRESPMessageError("ERR", fmt.Sprintf("invalid longitude,latitude pair %f,%f", longitude, latitude))
	}

	return longitude, latitude, nil
}

func parseGeoRadiusArgument(raw []byte) (float64, error) {
	radius, err := parseFloatArgument(raw)
	if err != nil {
		return 0, err
	}
	if radius < 0 {
		return 0, newRESPMessageError("ERR", "radius cannot be negative")
	}

	return radius, nil
}

func parseGeoUnitArgument(raw []byte) (float64, error) {
	switch strings.ToLower(string(raw)) {
	case "m":
		return 1, nil
	case "km":
		return geoMetersPerKilometer, nil
	case "mi":
		return geoMetersPerMile, nil
	case "ft":
		return geoMetersPerFoot, nil
	default:
		return 0, newRESPMessageError("ERR", "unsupported unit provided. please use m, km, ft, mi")
	}
}

func geohashEncode(longitude, latitude float64) uint64 {
	longitudeOffset := (longitude - geoLongitudeMin) / (geoLongitudeMax - geoLongitudeMin)
	latitudeOffset := (latitude - geoLatitudeMin) / (geoLatitudeMax - geoLatitudeMin)

	scale := float64(uint64(1) << geoStep)
	return interleave64(uint32(latitudeOffset*scale), uint32(longitudeOffset*scale))
}

// geohashBits converts a sorted-set score to geohash bits. Scores outside the
// encodable range (written by plain ZADD on the same key) map to zero, because
// Go's out-of-range float-to-integer conversion is platform-dependent.
func geohashBits(score float64) uint64 {
	if score < 0 || score >= float64(uint64(1)<<(2*geoStep+2)) {
		return 0
	}
	return uint64(score)
}

func geohashDecode(bits uint64) (longitude, latitude float64) {
	latitudeBits, longitudeBits := deinterleave64(bits)

	scale := float64(uint64(1) << geoStep)
	latitudeMin := geoLatitudeMin + (float64(latitudeBits)/scale)*(geoLatitudeMax-geoLatitudeMin)
	latitudeMax := geoLatitudeMin + (float64(latitudeBits+1)/scale)*(geoLatitudeMax-geoLatitudeMin)
	longitudeMin := geoLongitudeMin + (float64(longitudeBits)/scale)*(geoLongitudeMax-geoLongitudeMin)
	longitudeMax := geoLongitudeMin + (float64(longitudeBits+1)/scale)*(geoLongitudeMax-geoLongitudeMin)

	return (longitudeMin + longitudeMax) / 2, (latitudeMin + latitudeMax) / 2
}

// interleave64 spreads the bits of x into even positions and y into odd
// positions, producing the Morton code used as the sorted-set score.
func interleave64(x, y uint32) uint64 {
	even := spreadBits(x)
	odd := spreadBits(y)
	return even | (odd << 1)
}

func deinterleave64(interleaved uint64) (x, y uint32) {
	return squashBits(interleaved), squashBits(interleaved >> 1)
}

func spreadBits(value uint32) uint64 {
	spread := uint64(value)
	spread = (spread | (spread << 16)) & 0x0000FFFF0000FFFF
	spread = (spread | (spread << 8)) & 0x00FF00FF00FF00FF
	spread = (spread | (spread << 4)) & 0x0F0F0F0F0F0F0F0F
	spread = (spread | (spread << 2)) & 0x3333333333333333
	spread = (spread | (spread << 1)) & 0x5555555555555555
	return spread
}

func squashBits(spread uint64) uint32 {
	value := spread & 0x5555555555555555
	value = (value | (value >> 1)) & 0x3333333333333333
	value = (value | (value >> 2)) & 0x0F0F0F0F0F0F0F0F
	value = (value | (value >> 4)) & 0x00FF00FF00FF00FF
	value = (value | (value >> 8)) & 0x0000FFFF0000FFFF
	value = (value | (value >> 16)) & 0x00000000FFFFFFFF
	return uint32(value)
}

func geoDistanceMeters(longitude1, latitude1, longitude2, latitude2 float64) float64 {
	longitude1Rad := degreesToRadians(longitude1)
	longitude2Rad := degreesToRadians(longitude2)
	v := math.Sin((longitude2Rad - longitude1Rad) / 2)
	if v == 0 {
		return geoEarthRadiusMeters * math.Abs(degreesToRadians(latitude2)-degreesToRadians(latitude1))
	}

	latitude1Rad := degreesToRadians(latitude1)
	latitude2Rad := degreesToRadians(latitude2)
	u := math.Sin((latitude2Rad - latitude1Rad) / 2)
	a := u*u + math.Cos(latitude1Rad)*math.Cos(latitude2Rad)*v*v

	return 2 * geoEarthRadiusMeters * math.Asin(math.Sqrt(a))
}

func degreesToRadians(degrees float64) float64 {
	return degrees * (math.Pi / 180)
}

func radiansToDegrees(radians float64) float64 {
	return radians * (180 / math.Pi)
}
