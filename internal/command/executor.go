package command

import (
	"errors"
	"fmt"
)

var (
	// ErrSyntax reports malformed command syntax.
	ErrSyntax = errors.New("syntax error")
	// ErrExecWithoutMulti reports that EXEC was called outside a transaction.
	ErrExecWithoutMulti = errors.New("EXEC without MULTI")
	// ErrDiscardWithoutMulti reports that DISCARD was called outside a transaction.
	ErrDiscardWithoutMulti = errors.New("DISCARD without MULTI")
	// ErrMultiNested reports that MULTI was called while already inside a transaction.
	ErrMultiNested = errors.New("MULTI calls can not be nested")
	// ErrWatchInsideMulti reports that WATCH was called inside a MULTI block.
	ErrWatchInsideMulti = errors.New("WATCH inside MULTI is not allowed")
	// ErrExecAbort reports that EXEC aborted because queue-time validation failed.
	ErrExecAbort = errors.New("transaction discarded because of previous errors")
	// ErrNotAllowedInTransaction reports that a command that cannot run inside a
	// transaction was sent after MULTI.
	ErrNotAllowedInTransaction = errors.New("command not allowed inside a transaction")
	// ErrInvalidExpireTime reports an invalid EX/PX duration.
	ErrInvalidExpireTime = errors.New("invalid expire time in 'SET' command")
	// ErrInvalidStreamID reports that a stream ID could not be parsed.
	ErrInvalidStreamID = errors.New("invalid stream ID specified as stream command argument")
	// ErrStreamIDTooSmall reports that XADD was given a non-monotonic explicit ID.
	ErrStreamIDTooSmall = errors.New("stream ID is equal or smaller than the target stream top item")
	// ErrValueNotInteger reports that a value cannot be parsed as a 64-bit integer.
	ErrValueNotInteger = errors.New("value is not an integer or out of range")
	// ErrValueNotFloat reports that a value cannot be parsed as a float64.
	ErrValueNotFloat = errors.New("value is not a valid float")
	// ErrWrongType reports that a command targeted the wrong logical value type.
	ErrWrongType = errors.New("operation against a key holding the wrong kind of value")
	// ErrNotHyperLogLog reports that a string key does not hold a valid HyperLogLog value.
	ErrNotHyperLogLog = errors.New("key is not a valid HyperLogLog string value")
	// ErrNoAuth reports that the client must authenticate before running a command.
	ErrNoAuth = errors.New("authentication required")
	// ErrWrongPass reports that the supplied AUTH credentials were rejected.
	ErrWrongPass = errors.New("invalid username-password pair or user is disabled")
	// ErrAuthNotConfigured reports that AUTH was called without a configured password.
	ErrAuthNotConfigured = errors.New("AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct")
	// ErrSubscribedModeOnly reports that a subscribed client attempted a disallowed command.
	ErrSubscribedModeOnly = errors.New("only PING, SUBSCRIBE, and UNSUBSCRIBE are allowed in this context")
	// ErrSubscribeInsideMulti reports that subscribe state changes are not allowed inside MULTI.
	ErrSubscribeInsideMulti = errors.New("SUBSCRIBE and UNSUBSCRIBE inside MULTI are not allowed")
	// ErrMonitorModeOnly reports that a MONITOR client attempted a disallowed command.
	ErrMonitorModeOnly = errors.New("only PING is allowed in this context")
	// ErrOutOfMemory reports that write could not fit under maxmemory.
	ErrOutOfMemory = errors.New("command not allowed when used memory > 'maxmemory'")
)

// RESPError exposes the wire error prefix to use when returning a RESP error.
type RESPError interface {
	error
	RESPErrorPrefix() string
}

// ProtocolError represents malformed client protocol frames.
type ProtocolError struct {
	message string
}

// Error returns the protocol validation message.
func (e ProtocolError) Error() string {
	return e.message
}

// RESPErrorPrefix returns the Redis-compatible wire prefix for protocol errors.
func (e ProtocolError) RESPErrorPrefix() string {
	return "ERR"
}

type prefixedError struct {
	prefix  string
	message string
	cause   error
}

// Error returns the wire message without its RESP error prefix.
func (e *prefixedError) Error() string {
	return e.message
}

// Unwrap exposes the underlying cause to errors.Is and errors.As.
func (e *prefixedError) Unwrap() error {
	return e.cause
}

// RESPErrorPrefix returns the wire prefix this error is reported under.
func (e *prefixedError) RESPErrorPrefix() string {
	return e.prefix
}

// ErrProtocol creates a new protocol validation error.
func ErrProtocol(message string) error {
	return ProtocolError{message: message}
}

// ErrUnknownCommand reports an unsupported command name.
func ErrUnknownCommand(name string) error {
	return newRESPMessageError("ERR", fmt.Sprintf("unknown command %q", name))
}

// ErrSyntaxError reports a Redis-style syntax error.
func ErrSyntaxError() error {
	return newRESPWrappedError("ERR", ErrSyntax)
}

// ErrExecWithoutMultiError reports that EXEC was called without MULTI.
func ErrExecWithoutMultiError() error {
	return newRESPWrappedError("ERR", ErrExecWithoutMulti)
}

// ErrDiscardWithoutMultiError reports that DISCARD was called without MULTI.
func ErrDiscardWithoutMultiError() error {
	return newRESPWrappedError("ERR", ErrDiscardWithoutMulti)
}

// ErrMultiNestedError reports that MULTI was called while already in MULTI mode.
func ErrMultiNestedError() error {
	return newRESPWrappedError("ERR", ErrMultiNested)
}

// ErrWatchInsideMultiError reports that WATCH cannot be called after MULTI.
func ErrWatchInsideMultiError() error {
	return newRESPWrappedError("ERR", ErrWatchInsideMulti)
}

// ErrExecAbortError reports that EXEC refused to run after queue-time validation errors.
func ErrExecAbortError() error {
	return newRESPError("EXECABORT", "Transaction discarded because of previous errors.", ErrExecAbort)
}

// ErrNotAllowedInTransactionError reports that a command cannot be queued after
// MULTI. The transaction is marked dirty, so EXEC answers EXECABORT, as in Redis.
func ErrNotAllowedInTransactionError() error {
	return newRESPError("ERR", "Command not allowed inside a transaction", ErrNotAllowedInTransaction)
}

// ErrInvalidExpireTimeError reports an invalid SET expiry argument.
func ErrInvalidExpireTimeError() error {
	return newRESPWrappedError("ERR", ErrInvalidExpireTime)
}

// ErrInvalidStreamIDError reports that a stream ID argument could not be parsed.
func ErrInvalidStreamIDError() error {
	return newRESPWrappedError("ERR", ErrInvalidStreamID)
}

// ErrStreamIDTooSmallError reports that XADD received a non-monotonic explicit ID.
func ErrStreamIDTooSmallError() error {
	return newRESPError("ERR", "The ID specified in XADD is equal or smaller than the target stream top item", ErrStreamIDTooSmall)
}

// ErrValueNotIntegerError reports that a string value cannot be incremented.
func ErrValueNotIntegerError() error {
	return newRESPWrappedError("ERR", ErrValueNotInteger)
}

// ErrValueNotFloatError reports that a score could not be parsed as a float.
func ErrValueNotFloatError() error {
	return newRESPWrappedError("ERR", ErrValueNotFloat)
}

// ErrWrongTypeError reports a Redis-style wrong-type failure.
func ErrWrongTypeError() error {
	return newRESPError("WRONGTYPE", "Operation against a key holding the wrong kind of value", ErrWrongType)
}

// ErrNotHyperLogLogError reports that a string key does not hold a valid HyperLogLog value.
func ErrNotHyperLogLogError() error {
	return newRESPError("WRONGTYPE", "Key is not a valid HyperLogLog string value.", ErrNotHyperLogLog)
}

// ErrNoAuthError reports that the client must authenticate first.
func ErrNoAuthError() error {
	return newRESPError("NOAUTH", "Authentication required.", ErrNoAuth)
}

// ErrWrongPassError reports invalid AUTH credentials.
func ErrWrongPassError() error {
	return newRESPError("WRONGPASS", "invalid username-password pair or user is disabled.", ErrWrongPass)
}

// ErrAuthNotConfiguredError reports that AUTH is unavailable because no password is configured.
func ErrAuthNotConfiguredError() error {
	return newRESPWrappedError("ERR", ErrAuthNotConfigured)
}

// ErrSubscribedModeOnlyError reports that only pub/sub-safe commands are allowed.
func ErrSubscribedModeOnlyError() error {
	return newRESPWrappedError("ERR", ErrSubscribedModeOnly)
}

// ErrSubscribeInsideMultiError reports that subscription state changes are disallowed in MULTI.
func ErrSubscribeInsideMultiError() error {
	return newRESPWrappedError("ERR", ErrSubscribeInsideMulti)
}

// ErrMonitorModeOnlyError reports that only monitor-safe commands are allowed.
func ErrMonitorModeOnlyError() error {
	return newRESPWrappedError("ERR", ErrMonitorModeOnly)
}

// ErrOutOfMemoryError reports a Redis-style OOM failure.
func ErrOutOfMemoryError() error {
	return newRESPError("OOM", "command not allowed when used memory > 'maxmemory'", ErrOutOfMemory)
}

func wrongNumberOfArgumentsError(command string) error {
	return newRESPMessageError("ERR", fmt.Sprintf("wrong number of arguments for '%s' command", command))
}

func blockingNotSupportedError(command string) error {
	return newRESPMessageError("ERR", fmt.Sprintf("%s would block; blocking commands are not supported with event-loop networking", command))
}

func newRESPWrappedError(prefix string, cause error) error {
	return &prefixedError{prefix: prefix, message: cause.Error(), cause: cause}
}

func newRESPError(prefix, message string, cause error) error {
	return &prefixedError{prefix: prefix, message: message, cause: cause}
}

func newRESPMessageError(prefix, message string) error {
	return &prefixedError{prefix: prefix, message: message}
}
