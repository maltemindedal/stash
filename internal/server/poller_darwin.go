//go:build darwin

package server

import (
	"fmt"
	"syscall"
)

// newPoller constructs the macOS readiness poller backed by kqueue. A
// non-blocking pipe registered for EVFILT_READ provides cross-goroutine
// wakeups for Wake.
func newPoller() (poller, error) {
	kq, err := syscall.Kqueue()
	if err != nil {
		return nil, fmt.Errorf("kqueue: %w", err)
	}
	syscall.CloseOnExec(kq)

	var pipeFDs [2]int
	if err := syscall.Pipe(pipeFDs[:]); err != nil {
		closeIgnoringError(kq)
		return nil, fmt.Errorf("wake pipe: %w", err)
	}
	for _, fd := range pipeFDs {
		if err := syscall.SetNonblock(fd, true); err != nil {
			closeIgnoringError(kq)
			closeIgnoringError(pipeFDs[0])
			closeIgnoringError(pipeFDs[1])
			return nil, fmt.Errorf("wake pipe nonblock: %w", err)
		}
		syscall.CloseOnExec(fd)
	}

	p := &kqueuePoller{
		kq:       kq,
		wake:     wakePipe{readFD: pipeFDs[0], writeFD: pipeFDs[1]},
		kqEvents: make([]syscall.Kevent_t, pollerEventBatch),
	}
	if err := p.change(p.wake.readFD, syscall.EVFILT_READ, syscall.EV_ADD); err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("register wake pipe: %w", err)
	}

	return p, nil
}

type kqueuePoller struct {
	kq   int
	wake wakePipe

	kqEvents []syscall.Kevent_t
}

func (p *kqueuePoller) change(fd int, filter int16, flags uint16) error {
	changes := []syscall.Kevent_t{{
		Ident:  uint64(fd),
		Filter: filter,
		Flags:  flags,
	}}
	_, err := syscall.Kevent(p.kq, changes, nil, nil)
	return err
}

// setFilter adds or deletes one kqueue filter, treating deletion of an absent
// filter as a no-op so interest updates stay idempotent.
func (p *kqueuePoller) setFilter(fd int, filter int16, enable bool) error {
	if enable {
		return p.change(fd, filter, syscall.EV_ADD)
	}

	err := p.change(fd, filter, syscall.EV_DELETE)
	if err == syscall.ENOENT {
		return nil
	}
	return err
}

// Add registers fd with the kqueue for read readiness.
func (p *kqueuePoller) Add(fd int) error {
	return p.change(fd, syscall.EVFILT_READ, syscall.EV_ADD)
}

// Set rewrites fd's kqueue filters to match the requested interest set.
func (p *kqueuePoller) Set(fd int, readable, writable bool) error {
	if err := p.setFilter(fd, syscall.EVFILT_READ, readable); err != nil {
		return err
	}
	return p.setFilter(fd, syscall.EVFILT_WRITE, writable)
}

// Remove deletes fd's read and write filters from the kqueue.
func (p *kqueuePoller) Remove(fd int) error {
	readErr := p.setFilter(fd, syscall.EVFILT_READ, false)
	writeErr := p.setFilter(fd, syscall.EVFILT_WRITE, false)
	if readErr != nil {
		return readErr
	}
	return writeErr
}

// Wait blocks in kevent, retrying on EINTR, and translates the returned
// kevents into pollEvent entries. Wakeup-pipe readiness is drained and not
// reported to the caller.
func (p *kqueuePoller) Wait(events []pollEvent) (int, error) {
	for {
		n, err := syscall.Kevent(p.kq, nil, p.kqEvents, nil)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("kevent: %w", err)
		}

		out := 0
		for _, kqEvent := range p.kqEvents[:n] {
			fd := int(kqEvent.Ident)
			if fd == p.wake.readFD {
				p.wake.drain()
				continue
			}
			if out >= len(events) {
				break
			}
			events[out] = pollEvent{
				fd:       fd,
				readable: kqEvent.Filter == syscall.EVFILT_READ || kqEvent.Flags&syscall.EV_ERROR != 0,
				writable: kqEvent.Filter == syscall.EVFILT_WRITE,
			}
			out++
		}

		return out, nil
	}
}

// Wake writes to the wakeup pipe so a blocked Wait returns.
func (p *kqueuePoller) Wake() error {
	return p.wake.wake()
}

// Close releases the wakeup pipe and the kqueue descriptor.
func (p *kqueuePoller) Close() error {
	p.wake.close()
	return syscall.Close(p.kq)
}

// tcpKeepInterval is TCP_KEEPINTVL from <netinet/tcp.h>. The syscall package
// lacks it on some darwin architectures (darwin/amd64), so the net package
// defines it the same way for its own keepalive support.
const tcpKeepInterval = 0x101

// enableKeepalive turns on TCP keepalive for an accepted socket, probing after
// eventLoopKeepaliveSeconds of idleness and then at the same interval. macOS
// names the idle-time option TCP_KEEPALIVE. These are the calls the net package
// makes for the connections it accepts.
func enableKeepalive(fd int) error {
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1); err != nil {
		return fmt.Errorf("SO_KEEPALIVE: %w", err)
	}
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPALIVE, eventLoopKeepaliveSeconds); err != nil {
		return fmt.Errorf("TCP_KEEPALIVE: %w", err)
	}
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, tcpKeepInterval, eventLoopKeepaliveSeconds); err != nil {
		return fmt.Errorf("TCP_KEEPINTVL: %w", err)
	}
	return nil
}
