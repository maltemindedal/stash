// Package record decides where the command frames of an applied write go: to
// the append-only file, to the Replicas, or both, and what a failed append does
// to the write's reply and to its propagation. It is a leaf: it knows the RESP
// encoding and nothing of the server, the commands or the files it writes to.
package record

// Origin is who caused a request or a write, which decides what it may do and
// where its command frames go. The zero Origin is none of them: the command
// executor refuses a request without one.
type Origin uint8

const (
	// OriginClient is a client connection.
	OriginClient Origin = iota + 1
	// OriginMaster is the Master's replication stream, applied on a Replica.
	OriginMaster
	// OriginReplay is AOF replay at startup.
	OriginReplay
	// OriginServer is work the server does on its own: an eviction it makes
	// outside any request. It is never a request's Origin.
	OriginServer
)

// Propagates reports whether the Origin's writes are forwarded to Replicas.
func (o Origin) Propagates() bool { return o == OriginClient || o == OriginServer }

// RecordsSlowlog reports whether the request can be recorded in the Slowlog.
func (o Origin) RecordsSlowlog() bool { return o == OriginClient }

// NeedsAuth reports whether the request runs only for a client that has
// authenticated, on a server that requires a password.
func (o Origin) NeedsAuth() bool { return o == OriginClient }

// AnswersGetAck reports whether the request may answer REPLCONF GETACK.
func (o Origin) AnswersGetAck() bool { return o == OriginMaster }
