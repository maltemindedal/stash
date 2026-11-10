//go:build !linux && !darwin

package test

// networkingModes lists only the default mode: without a poller --event-loop
// falls back to it, and a second run would test the same code under another
// name.
var networkingModes = []networkingMode{
	{name: "goroutine per connection"},
}
