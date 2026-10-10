package command

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
)

// TestGeoBoundingDeltasUsesPolewardEdge verifies the longitude half-width is
// evaluated at the box's poleward edge (smaller cosine → wider box), not the
// center latitude, so a high-latitude GEORADIUS box cannot under-cover and drop
// in-radius members.
func TestGeoBoundingDeltasUsesPolewardEdge(t *testing.T) {
	const latitude = 70.0
	const radiusMeters = 100000.0

	_, deltaLongitude := geoBoundingDeltas(latitude, radiusMeters)

	// The previous (undersized) width evaluated cos at the center latitude.
	radiusRadians := radiusMeters / geoEarthRadiusMeters
	centerWidth := radiansToDegrees(math.Asin(math.Sin(radiusRadians) / math.Cos(degreesToRadians(latitude))))

	if deltaLongitude < centerWidth {
		t.Fatalf("deltaLongitude = %v, want >= poleward-edge width (>= center-latitude width %v)", deltaLongitude, centerWidth)
	}
}

const (
	palermoLongitude = 13.361389
	palermoLatitude  = 38.115556
	cataniaLongitude = 15.087269
	cataniaLatitude  = 37.502669
)

func TestGeohashEncodeDecode(t *testing.T) {
	t.Run("encodes Redis-compatible scores", func(t *testing.T) {
		tests := []struct {
			name      string
			longitude float64
			latitude  float64
			want      uint64
		}{
			{name: "Palermo", longitude: palermoLongitude, latitude: palermoLatitude, want: 3479099956230698},
			{name: "Catania", longitude: cataniaLongitude, latitude: cataniaLatitude, want: 3479447370796909},
		}
		for _, tt := range tests {
			if got := geohashEncode(tt.longitude, tt.latitude); got != tt.want {
				t.Fatalf("geohashEncode(%s) = %d, want %d", tt.name, got, tt.want)
			}
		}
	})

	t.Run("out-of-range scores map to zero bits deterministically", func(t *testing.T) {
		if got := geohashBits(-1); got != 0 {
			t.Fatalf("geohashBits(-1) = %d, want 0", got)
		}
		if got := geohashBits(1e300); got != 0 {
			t.Fatalf("geohashBits(1e300) = %d, want 0", got)
		}
		boundary := float64(geohashEncode(geoLongitudeMax, geoLatitudeMax))
		if got := geohashBits(boundary); got != uint64(boundary) {
			t.Fatalf("geohashBits(boundary encode) = %d, want %d", got, uint64(boundary))
		}
	})

	t.Run("decode returns the encoded cell center", func(t *testing.T) {
		longitude, latitude := geohashDecode(geohashEncode(palermoLongitude, palermoLatitude))
		if math.Abs(longitude-palermoLongitude) > 0.00001 {
			t.Fatalf("decoded longitude = %v, want within 0.00001 of %v", longitude, palermoLongitude)
		}
		if math.Abs(latitude-palermoLatitude) > 0.00001 {
			t.Fatalf("decoded latitude = %v, want within 0.00001 of %v", latitude, palermoLatitude)
		}
	})
}

func TestExecutorGeoAdd(t *testing.T) {
	t.Run("adds new members and returns added count", func(t *testing.T) {
		executor := newTestExecutor()
		value, err := executor.Execute(clientContext(executor), geoAddSicilyRequest())
		if err != nil {
			t.Fatalf("GEOADD error = %v", err)
		}
		assertValueEqual(t, value, protocol.Integer{Value: 2})
	})

	t.Run("updating an existing member returns zero", func(t *testing.T) {
		executor := newTestExecutor()
		if _, err := executor.Execute(clientContext(executor), geoAddSicilyRequest()); err != nil {
			t.Fatalf("GEOADD error = %v", err)
		}

		value, err := executor.Execute(clientContext(executor), requestValue("GEOADD", "Sicily", "13.5", "38.2", "Palermo"))
		if err != nil {
			t.Fatalf("GEOADD update error = %v", err)
		}
		assertValueEqual(t, value, protocol.Integer{Value: 0})
	})

	t.Run("stores members as sorted-set data", func(t *testing.T) {
		executor := newTestExecutor()
		if _, err := executor.Execute(clientContext(executor), geoAddSicilyRequest()); err != nil {
			t.Fatalf("GEOADD error = %v", err)
		}

		value, err := executor.Execute(clientContext(executor), requestValue("ZRANGE", "Sicily", "0", "-1"))
		if err != nil {
			t.Fatalf("ZRANGE error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
			protocol.TextBulkString{Value: "Palermo"},
			protocol.TextBulkString{Value: "Catania"},
		}})
	})

	t.Run("rejects out-of-range coordinates", func(t *testing.T) {
		executor := newTestExecutor()
		_, err := executor.Execute(clientContext(executor), requestValue("GEOADD", "Sicily", "200", "40", "nowhere"))
		if err == nil {
			t.Fatal("GEOADD error = nil, want invalid coordinates error")
		}
		if got, want := err.Error(), "invalid longitude,latitude pair 200.000000,40.000000"; got != want {
			t.Fatalf("GEOADD error = %q, want %q", got, want)
		}
	})

	t.Run("rejects non-numeric coordinates", func(t *testing.T) {
		executor := newTestExecutor()
		_, err := executor.Execute(clientContext(executor), requestValue("GEOADD", "Sicily", "east", "38", "Palermo"))
		if err == nil {
			t.Fatal("GEOADD error = nil, want float parse error")
		}
		assertRESPPrefix(t, err, "ERR")
	})

	t.Run("rejects incomplete coordinate triples", func(t *testing.T) {
		executor := newTestExecutor()
		_, err := executor.Execute(clientContext(executor), requestValue("GEOADD", "Sicily", "13.361389", "38.115556"))
		if err == nil {
			t.Fatal("GEOADD error = nil, want wrong number of arguments error")
		}
		assertRESPPrefix(t, err, "ERR")
	})

	t.Run("fails against a non-zset key", func(t *testing.T) {
		executor := newTestExecutor()
		if _, err := executor.Execute(clientContext(executor), requestValue("SET", "Sicily", "island")); err != nil {
			t.Fatalf("SET error = %v", err)
		}

		_, err := executor.Execute(clientContext(executor), requestValue("GEOADD", "Sicily", "13.361389", "38.115556", "Palermo"))
		if err == nil {
			t.Fatal("GEOADD error = nil, want WRONGTYPE error")
		}
		assertRESPPrefix(t, err, "WRONGTYPE")
	})
}

func TestExecutorGeoDist(t *testing.T) {
	t.Run("returns distances in supported units", func(t *testing.T) {
		tests := []struct {
			name string
			args []string
			want string
		}{
			{name: "default meters", args: []string{"GEODIST", "Sicily", "Palermo", "Catania"}, want: "166274.1516"},
			{name: "explicit meters", args: []string{"GEODIST", "Sicily", "Palermo", "Catania", "m"}, want: "166274.1516"},
			{name: "kilometers", args: []string{"GEODIST", "Sicily", "Palermo", "Catania", "km"}, want: "166.2742"},
			{name: "miles", args: []string{"GEODIST", "Sicily", "Palermo", "Catania", "mi"}, want: "103.3182"},
			{name: "feet", args: []string{"GEODIST", "Sicily", "Palermo", "Catania", "ft"}, want: "545518.8700"},
			{name: "uppercase unit", args: []string{"GEODIST", "Sicily", "Palermo", "Catania", "KM"}, want: "166.2742"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				executor := newTestExecutor()
				if _, err := executor.Execute(clientContext(executor), geoAddSicilyRequest()); err != nil {
					t.Fatalf("GEOADD error = %v", err)
				}

				value, err := executor.Execute(clientContext(executor), requestValue(tt.args...))
				if err != nil {
					t.Fatalf("GEODIST error = %v", err)
				}
				assertValueEqual(t, value, protocol.TextBulkString{Value: tt.want})
			})
		}
	})

	t.Run("returns null bulk string when a member is missing", func(t *testing.T) {
		executor := newTestExecutor()
		if _, err := executor.Execute(clientContext(executor), geoAddSicilyRequest()); err != nil {
			t.Fatalf("GEOADD error = %v", err)
		}

		value, err := executor.Execute(clientContext(executor), requestValue("GEODIST", "Sicily", "Palermo", "Messina"))
		if err != nil {
			t.Fatalf("GEODIST error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Null: true})
	})

	t.Run("returns null bulk string for a missing key", func(t *testing.T) {
		executor := newTestExecutor()
		value, err := executor.Execute(clientContext(executor), requestValue("GEODIST", "missing", "Palermo", "Catania"))
		if err != nil {
			t.Fatalf("GEODIST error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Null: true})
	})

	t.Run("rejects unsupported units", func(t *testing.T) {
		executor := newTestExecutor()
		if _, err := executor.Execute(clientContext(executor), geoAddSicilyRequest()); err != nil {
			t.Fatalf("GEOADD error = %v", err)
		}

		_, err := executor.Execute(clientContext(executor), requestValue("GEODIST", "Sicily", "Palermo", "Catania", "yd"))
		if err == nil {
			t.Fatal("GEODIST error = nil, want unsupported unit error")
		}
		if got, want := err.Error(), "unsupported unit provided. please use m, km, ft, mi"; got != want {
			t.Fatalf("GEODIST error = %q, want %q", got, want)
		}
	})

	t.Run("fails against a non-zset key", func(t *testing.T) {
		executor := newTestExecutor()
		if _, err := executor.Execute(clientContext(executor), requestValue("SET", "Sicily", "island")); err != nil {
			t.Fatalf("SET error = %v", err)
		}

		_, err := executor.Execute(clientContext(executor), requestValue("GEODIST", "Sicily", "Palermo", "Catania"))
		if err == nil {
			t.Fatal("GEODIST error = nil, want WRONGTYPE error")
		}
		assertRESPPrefix(t, err, "WRONGTYPE")
	})

	t.Run("treats non-geo scores deterministically", func(t *testing.T) {
		executor := newTestExecutor()
		if _, err := executor.Execute(clientContext(executor), requestValue("ZADD", "mixed", "-1", "a", "-2", "b")); err != nil {
			t.Fatalf("ZADD error = %v", err)
		}

		value, err := executor.Execute(clientContext(executor), requestValue("GEODIST", "mixed", "a", "b"))
		if err != nil {
			t.Fatalf("GEODIST error = %v", err)
		}
		assertValueEqual(t, value, protocol.TextBulkString{Value: "0.0000"})
	})
}

func TestExecutorGeoRadius(t *testing.T) {
	t.Run("returns members within the radius", func(t *testing.T) {
		tests := []struct {
			name string
			args []string
			want []string
		}{
			{name: "wide radius matches all", args: []string{"GEORADIUS", "Sicily", "15", "37", "200", "km"}, want: []string{"Palermo", "Catania"}},
			{name: "narrow radius filters", args: []string{"GEORADIUS", "Sicily", "15", "37", "100", "km"}, want: []string{"Catania"}},
			{name: "meter radius", args: []string{"GEORADIUS", "Sicily", "15", "37", "100000", "m"}, want: []string{"Catania"}},
			{name: "tiny radius matches nothing", args: []string{"GEORADIUS", "Sicily", "15", "37", "1", "m"}, want: []string{}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				executor := newTestExecutor()
				if _, err := executor.Execute(clientContext(executor), geoAddSicilyRequest()); err != nil {
					t.Fatalf("GEOADD error = %v", err)
				}

				value, err := executor.Execute(clientContext(executor), requestValue(tt.args...))
				if err != nil {
					t.Fatalf("GEORADIUS error = %v", err)
				}

				wantElements := make([]protocol.Value, 0, len(tt.want))
				for _, member := range tt.want {
					wantElements = append(wantElements, protocol.TextBulkString{Value: member})
				}
				assertValueEqual(t, value, protocol.Array{Elements: wantElements})
			})
		}
	})

	t.Run("returns empty array for a missing key", func(t *testing.T) {
		executor := newTestExecutor()
		value, err := executor.Execute(clientContext(executor), requestValue("GEORADIUS", "missing", "15", "37", "200", "km"))
		if err != nil {
			t.Fatalf("GEORADIUS error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{}})
	})

	t.Run("rejects negative radius", func(t *testing.T) {
		executor := newTestExecutor()
		_, err := executor.Execute(clientContext(executor), requestValue("GEORADIUS", "Sicily", "15", "37", "-1", "km"))
		if err == nil {
			t.Fatal("GEORADIUS error = nil, want negative radius error")
		}
		if got, want := err.Error(), "radius cannot be negative"; got != want {
			t.Fatalf("GEORADIUS error = %q, want %q", got, want)
		}
	})

	t.Run("rejects out-of-range center coordinates", func(t *testing.T) {
		executor := newTestExecutor()
		_, err := executor.Execute(clientContext(executor), requestValue("GEORADIUS", "Sicily", "200", "37", "1", "km"))
		if err == nil {
			t.Fatal("GEORADIUS error = nil, want invalid coordinates error")
		}
		assertRESPPrefix(t, err, "ERR")
	})

	t.Run("rejects unsupported units", func(t *testing.T) {
		executor := newTestExecutor()
		_, err := executor.Execute(clientContext(executor), requestValue("GEORADIUS", "Sicily", "15", "37", "200", "yd"))
		if err == nil {
			t.Fatal("GEORADIUS error = nil, want unsupported unit error")
		}
		assertRESPPrefix(t, err, "ERR")
	})

	t.Run("rejects unsupported modifiers", func(t *testing.T) {
		executor := newTestExecutor()
		_, err := executor.Execute(clientContext(executor), requestValue("GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHCOORD"))
		if err == nil {
			t.Fatal("GEORADIUS error = nil, want syntax error")
		}
		assertRESPPrefix(t, err, "ERR")
	})

	t.Run("fails against a non-zset key", func(t *testing.T) {
		executor := newTestExecutor()
		if _, err := executor.Execute(clientContext(executor), requestValue("SET", "Sicily", "island")); err != nil {
			t.Fatalf("SET error = %v", err)
		}

		_, err := executor.Execute(clientContext(executor), requestValue("GEORADIUS", "Sicily", "15", "37", "200", "km"))
		if err == nil {
			t.Fatal("GEORADIUS error = nil, want WRONGTYPE error")
		}
		assertRESPPrefix(t, err, "WRONGTYPE")
	})

	t.Run("treats non-geo scores deterministically", func(t *testing.T) {
		executor := newTestExecutor()
		if _, err := executor.Execute(clientContext(executor), requestValue("ZADD", "mixed", "-1", "negative", "1e300", "huge")); err != nil {
			t.Fatalf("ZADD error = %v", err)
		}
		if _, err := executor.Execute(clientContext(executor), requestValue("GEOADD", "mixed", "13.361389", "38.115556", "Palermo")); err != nil {
			t.Fatalf("GEOADD error = %v", err)
		}

		// Out-of-range scores map to zero geohash bits, which decode to the
		// south-west corner cell of the coordinate space.
		cornerLongitude, cornerLatitude := geohashDecode(0)
		value, err := executor.Execute(clientContext(executor), requestValue(
			"GEORADIUS", "mixed",
			strconv.FormatFloat(cornerLongitude, 'f', -1, 64),
			strconv.FormatFloat(cornerLatitude, 'f', -1, 64),
			"1000", "m",
		))
		if err != nil {
			t.Fatalf("GEORADIUS corner error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
			protocol.TextBulkString{Value: "negative"},
			protocol.TextBulkString{Value: "huge"},
		}})

		value, err = executor.Execute(clientContext(executor), requestValue("GEORADIUS", "mixed", "13.361389", "38.115556", "1", "km"))
		if err != nil {
			t.Fatalf("GEORADIUS Palermo error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
			protocol.TextBulkString{Value: "Palermo"},
		}})
	})
}

// TestExecutorGeoRadiusMatchesFullScan checks the geohash score-range pruning
// against a reference full scan replicating the previous GEORADIUS
// implementation: every member decoded and distance-filtered in sorted-set
// order.
func TestExecutorGeoRadiusMatchesFullScan(t *testing.T) {
	points := geoWorldGridPoints()

	executor := newTestExecutor()
	args := []string{"GEOADD", "world"}
	for _, point := range points {
		args = append(args,
			strconv.FormatFloat(point.longitude, 'f', -1, 64),
			strconv.FormatFloat(point.latitude, 'f', -1, 64),
			point.member,
		)
	}
	if _, err := executor.Execute(clientContext(executor), requestValue(args...)); err != nil {
		t.Fatalf("GEOADD error = %v", err)
	}

	queries := []struct {
		name           string
		longitude      float64
		latitude       float64
		radius         float64
		wantMinMatches int
	}{
		{name: "zero radius", longitude: 30, latitude: 20, radius: 0},
		{name: "tiny radius", longitude: 15, latitude: 37, radius: 1},
		{name: "city radius", longitude: 15, latitude: 37, radius: 200_000},
		{name: "regional radius", longitude: -122, latitude: 47, radius: 1_500_000, wantMinMatches: 1},
		{name: "hemispheric radius", longitude: 0, latitude: 0, radius: 9_000_000, wantMinMatches: 10},
		{name: "global radius", longitude: 10, latitude: 10, radius: 30_000_000, wantMinMatches: len(points)},
		{name: "antimeridian crossing", longitude: -180, latitude: 10, radius: 5_000, wantMinMatches: 2},
		{name: "near north pole", longitude: 0, latitude: 85, radius: 300_000, wantMinMatches: 1},
		{name: "boundary corner", longitude: 179.9, latitude: 85, radius: 100_000, wantMinMatches: 1},
		{name: "south boundary", longitude: -179, latitude: -85, radius: 700_000, wantMinMatches: 1},
	}
	for _, query := range queries {
		t.Run(query.name, func(t *testing.T) {
			value, err := executor.Execute(clientContext(executor), requestValue(
				"GEORADIUS", "world",
				strconv.FormatFloat(query.longitude, 'f', -1, 64),
				strconv.FormatFloat(query.latitude, 'f', -1, 64),
				strconv.FormatFloat(query.radius, 'f', -1, 64),
				"m",
			))
			if err != nil {
				t.Fatalf("GEORADIUS error = %v", err)
			}

			want := fullScanGeoRadiusMembers(points, query.longitude, query.latitude, query.radius)
			if len(want) < query.wantMinMatches {
				t.Fatalf("full-scan reference matched %d members, want at least %d", len(want), query.wantMinMatches)
			}
			wantElements := make([]protocol.Value, 0, len(want))
			for _, member := range want {
				wantElements = append(wantElements, protocol.TextBulkString{Value: member})
			}
			assertValueEqual(t, value, protocol.Array{Elements: wantElements})
		})
	}
}

type geoTestPoint struct {
	member    string
	longitude float64
	latitude  float64
}

// geoWorldGridPoints spans a coarse world grid plus positions that stress the
// antimeridian wrap and the exact coordinate range boundaries, whose scores
// overflow the 52-bit geohash encoding.
func geoWorldGridPoints() []geoTestPoint {
	points := make([]geoTestPoint, 0, 128)
	for longitude := -180.0; longitude < 180.0; longitude += 30 {
		for latitude := -80.0; latitude <= 80.0; latitude += 20 {
			points = append(points, geoTestPoint{
				member:    fmt.Sprintf("grid:%g:%g", longitude, latitude),
				longitude: longitude,
				latitude:  latitude,
			})
		}
	}

	return append(points,
		geoTestPoint{member: "antimeridian-east", longitude: 179.99, latitude: 10},
		geoTestPoint{member: "antimeridian-west", longitude: -179.99, latitude: 10},
		geoTestPoint{member: "north-edge", longitude: 12.5, latitude: geoLatitudeMax},
		geoTestPoint{member: "east-edge", longitude: geoLongitudeMax, latitude: 37.5},
		geoTestPoint{member: "max-corner", longitude: geoLongitudeMax, latitude: geoLatitudeMax},
	)
}

func fullScanGeoRadiusMembers(points []geoTestPoint, longitude, latitude, radiusMeters float64) []string {
	type scoredMember struct {
		member string
		score  float64
	}

	scored := make([]scoredMember, 0, len(points))
	for _, point := range points {
		scored = append(scored, scoredMember{
			member: point.member,
			score:  float64(geohashEncode(point.longitude, point.latitude)),
		})
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score < scored[j].score
		}
		return scored[i].member < scored[j].member
	})

	matches := make([]string, 0, len(scored))
	for _, candidate := range scored {
		memberLongitude, memberLatitude := geohashDecode(geohashBits(candidate.score))
		if geoDistanceMeters(longitude, latitude, memberLongitude, memberLatitude) <= radiusMeters {
			matches = append(matches, candidate.member)
		}
	}
	return matches
}

func geoAddSicilyRequest() protocol.Value {
	return requestValue(
		"GEOADD", "Sicily",
		"13.361389", "38.115556", "Palermo",
		"15.087269", "37.502669", "Catania",
	)
}
