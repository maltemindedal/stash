// Package replication is both ends of a replication link.
//
// Master is the component through which a Master serves its Replicas: it
// counts the replication offset, attaches each Replica at one attach cut,
// queues the stream for it on a feed of its own, and tracks what it has
// acknowledged. Link is a Replica's side: the handshake, the full resync and
// the stream, over any connection, and Follow keeps a Link attached across
// reconnects.
//
// The package knows the RESP encoding, the RDB snapshot and the Store a full
// resync copies. It knows nothing of the server's connections or the command
// executor: a Replica's connection is an Output and an io.Closer, the
// sequencer's attach cut is a function Attach is given, and the Replica's
// Store is the two-method ReplicaStore.
package replication
