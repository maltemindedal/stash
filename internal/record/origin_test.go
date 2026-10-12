package record

import "testing"

func TestEachOriginHasOnlyItsCapabilities(t *testing.T) {
	type capabilities struct {
		propagates, recordsSlowlog, needsAuth, answersGetAck bool
	}
	tests := []struct {
		name   string
		origin Origin
		want   capabilities
	}{
		{name: "a client's request is propagated, recorded in the Slowlog and needs AUTH", origin: OriginClient, want: capabilities{propagates: true, recordsSlowlog: true, needsAuth: true}},
		{name: "the Master's stream only answers GETACK", origin: OriginMaster, want: capabilities{answersGetAck: true}},
		{name: "AOF replay has none of them", origin: OriginReplay},
		{name: "the server's own writes are only propagated", origin: OriginServer, want: capabilities{propagates: true}},
		{name: "the zero Origin has none of them", origin: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := capabilities{
				propagates:     tt.origin.Propagates(),
				recordsSlowlog: tt.origin.RecordsSlowlog(),
				needsAuth:      tt.origin.NeedsAuth(),
				answersGetAck:  tt.origin.AnswersGetAck(),
			}
			if got != tt.want {
				t.Fatalf("Origin %d capabilities = %+v, want %+v", tt.origin, got, tt.want)
			}
		})
	}
}
