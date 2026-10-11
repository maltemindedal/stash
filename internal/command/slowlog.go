package command

import (
	"context"
	"strconv"
	"strings"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
)

func (e *Executor) handleSlowlog(_ context.Context, request *Request) (protocol.Value, error) {
	subcommand := strings.ToUpper(string(request.Args[0]))
	switch subcommand {
	case "GET":
		limit := -1
		if len(request.Args) == 2 {
			parsed, err := parseIntArgument(request.Args[1])
			if err != nil || parsed < 0 {
				return nil, ErrValueNotIntegerError()
			}
			limit = parsed
		}
		return slowlogEntriesResponse(e.slowlogEntries(limit)), nil
	case "LEN":
		return protocol.Integer{Value: int64(e.slowlogRegistry.Len())}, nil
	case "RESET":
		e.slowlogRegistry.Reset()
		return protocol.SimpleString{Value: "OK"}, nil
	default:
		return nil, ErrSyntaxError()
	}
}

func validateSlowlogRequest(request *Request) error {
	if len(request.Args) == 0 {
		return wrongNumberOfArgumentsError("SLOWLOG")
	}

	subcommand := strings.ToUpper(string(request.Args[0]))
	switch subcommand {
	case "GET":
		if len(request.Args) > 2 {
			return wrongNumberOfArgumentsError("SLOWLOG")
		}
		if len(request.Args) == 2 {
			parsed, err := parseIntegerArgument(request.Args[1])
			if err != nil || parsed < 0 {
				return ErrValueNotIntegerError()
			}
		}
	case "LEN", "RESET":
		if len(request.Args) != 1 {
			return wrongNumberOfArgumentsError("SLOWLOG")
		}
	default:
		return ErrSyntaxError()
	}
	return nil
}

func (e *Executor) slowlogEntries(limit int) []server.SlowlogEntry {
	return e.slowlogRegistry.Entries(limit)
}

func slowlogEntriesResponse(entries []server.SlowlogEntry) protocol.Array {
	elements := make([]protocol.Value, 0, len(entries))
	for _, entry := range entries {
		argv := make([]protocol.Value, 0, len(entry.Command))
		for _, token := range entry.Command {
			argv = append(argv, protocol.TextBulkString{Value: token})
		}

		elements = append(elements, protocol.Array{Elements: []protocol.Value{
			protocol.Integer{Value: entry.ID},
			protocol.Integer{Value: entry.Timestamp.Unix()},
			protocol.Integer{Value: entry.Duration.Microseconds()},
			protocol.Array{Elements: argv},
			protocol.TextBulkString{Value: entry.ClientAddr},
			protocol.TextBulkString{Value: ""},
		}})
	}

	return protocol.Array{Elements: elements}
}

// Limits on what one slowlog entry keeps, the same as Redis': a command is
// stored with at most this many tokens (its name included), and each token is cut
// to this many bytes. The log holds 128 entries, so without them a slow command
// carrying a large value pins that value in memory for as long as it stays there.
const (
	slowlogMaxTokens     = 32
	slowlogMaxTokenBytes = 128
)

// requestTokens returns the command as the slowlog stores it. When the command
// has more than slowlogMaxTokens tokens the last kept slot reports how many were
// left out, counting the one it replaces; a longer token keeps its first
// slowlogMaxTokenBytes bytes and the count of the rest.
func requestTokens(request *Request) []string {
	if request == nil {
		return nil
	}

	total := len(request.Args) + 1
	kept := min(total, slowlogMaxTokens)
	sensitive := isSensitiveSlowlogCommand(request.Name)

	tokens := make([]string, 0, kept)
	for i := 0; i < kept; i++ {
		switch {
		case i == kept-1 && kept != total:
			tokens = append(tokens, "... ("+strconv.Itoa(total-kept+1)+" more arguments)")
		case i == 0:
			tokens = append(tokens, request.Name)
		case sensitive:
			tokens = append(tokens, "[redacted]")
		default:
			tokens = append(tokens, truncateSlowlogToken(request.Args[i-1]))
		}
	}
	return tokens
}

func truncateSlowlogToken(arg []byte) string {
	if len(arg) <= slowlogMaxTokenBytes {
		return string(arg)
	}
	return string(arg[:slowlogMaxTokenBytes]) + "... (" + strconv.Itoa(len(arg)-slowlogMaxTokenBytes) + " more bytes)"
}

func isSensitiveSlowlogCommand(name string) bool {
	switch name {
	case "AUTH":
		return true
	default:
		return false
	}
}
