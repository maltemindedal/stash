package proto

// The measures. Each is the size of one region of the connection's output,
// named for that region. A rule is a comparison of one measure with a limit;
// a new rule picks a measure here or adds one, and the model test
// (model_test.go) defines every measure again over individual bytes.

// UntilReply is how many bytes the peer must still read before it has the
// latest Reply: the unwritten part of that Reply and whatever is ahead of it.
// Nothing pushed after the latest Reply counts.
func (m Marks) UntilReply() int64 {
	if m.ReplyEnd <= m.Written {
		return 0
	}
	return int64(m.ReplyEnd - m.Written)
}

// AfterReply is how many unwritten bytes lie behind the latest Reply, held
// ones included: for a Replica, its stream.
func (m Marks) AfterReply() int64 {
	return int64(m.Queued-max(m.ReplyEnd, m.Written)) + m.Held
}

// Unwritten is every byte that waits for the connection, held ones included.
func (m Marks) Unwritten() int64 {
	return int64(m.Queued-m.Written) + m.Held
}

// OnOffer is how many bytes a writer has offered the socket that it has not
// taken yet.
func (m Marks) OnOffer() int64 {
	return int64(m.Offered - m.Written)
}

// The rules. Each mode of networking asks these and nothing else.

// MayReadNext reports whether the connection's next request may be read and
// run. It is the same rule in both networking modes: the goroutine mode waits
// in Flush while it is false, the event loop drops read interest.
func MayReadNext(m Marks, lim Limits) bool {
	return m.UntilReply() <= lim.HighWater
}

// Drained reports whether every Reply queued so far is written, which is when
// a connection that ran its last request may be closed.
func Drained(m Marks) bool {
	return m.UntilReply() == 0
}
