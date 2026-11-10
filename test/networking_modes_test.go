package test

import (
	"testing"

	"github.com/maltemindedal/stash/internal/config"
)

// networkingMode is one of the two ways the server serves its connections.
type networkingMode struct {
	name      string
	eventLoop bool
}

// modeRunner is a *testing.T or a *testing.B: something that runs named
// subtests of its own kind.
type modeRunner[T any] interface {
	testing.TB
	Run(name string, body func(T)) bool
}

// forEachNetworkingMode runs body once per networking mode this platform has
// (networkingModes), each time as a subtest named after the mode and with cfg
// set to it. body starts its own server from that configuration, so every mode
// gets a fresh one.
func forEachNetworkingMode[T modeRunner[T]](t T, cfg config.Config, body func(T, config.Config)) {
	t.Helper()

	for _, mode := range networkingModes {
		// Go 1.21 shares the loop variable between iterations.
		mode := mode
		t.Run(mode.name, func(t T) {
			modeCfg := cfg
			modeCfg.EventLoop = mode.eventLoop
			body(t, modeCfg)
		})
	}
}
