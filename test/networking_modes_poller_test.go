//go:build linux || darwin

package test

// networkingModes lists both modes where the event loop has a poller.
var networkingModes = []networkingMode{
	{name: "goroutine per connection"},
	{name: "event loop", eventLoop: true},
}
