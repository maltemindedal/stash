//go:build linux

package server

import (
	"fmt"
	"syscall"
)

// newPoller constructs the Linux readiness poller backed by epoll. A
// non-blocking pipe registered in the epoll set provides cross-goroutine
// wakeups for Wake.
func newPoller() (poller, error) {
	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll_create1: %w", err)
	}

	var pipeFDs [2]int
	if err := syscall.Pipe2(pipeFDs[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		closeIgnoringError(epfd)
		return nil, fmt.Errorf("wake pipe: %w", err)
	}

	p := &epollPoller{
		epfd:     epfd,
		wake:     wakePipe{readFD: pipeFDs[0], writeFD: pipeFDs[1]},
		epEvents: make([]syscall.EpollEvent, pollerEventBatch),
	}
	wakeEvent := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(p.wake.readFD)}
	if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, p.wake.readFD, &wakeEvent); err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("register wake pipe: %w", err)
	}

	return p, nil
}

type epollPoller struct {
	epfd int
	wake wakePipe

	epEvents []syscall.EpollEvent
}

const epollReadEvents = syscall.EPOLLIN | syscall.EPOLLRDHUP

func epollEventBits(readable, writable bool) uint32 {
	var events uint32
	if readable {
		events |= epollReadEvents
	}
	if writable {
		events |= syscall.EPOLLOUT
	}
	return events
}

// Add registers fd with the epoll set for read readiness.
func (p *epollPoller) Add(fd int) error {
	event := syscall.EpollEvent{Events: epollEventBits(true, false), Fd: int32(fd)}
	return syscall.EpollCtl(p.epfd, syscall.EPOLL_CTL_ADD, fd, &event)
}

// Set rewrites fd's epoll interest mask via EPOLL_CTL_MOD.
func (p *epollPoller) Set(fd int, readable, writable bool) error {
	event := syscall.EpollEvent{Events: epollEventBits(readable, writable), Fd: int32(fd)}
	return syscall.EpollCtl(p.epfd, syscall.EPOLL_CTL_MOD, fd, &event)
}

// Remove deregisters fd via EPOLL_CTL_DEL.
func (p *epollPoller) Remove(fd int) error {
	return syscall.EpollCtl(p.epfd, syscall.EPOLL_CTL_DEL, fd, nil)
}

// Wait blocks in epoll_wait, retrying on EINTR, and translates the returned
// epoll events into pollEvent entries. Wakeup-pipe readiness is drained and
// not reported to the caller.
func (p *epollPoller) Wait(events []pollEvent) (int, error) {
	for {
		n, err := syscall.EpollWait(p.epfd, p.epEvents, -1)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("epoll_wait: %w", err)
		}

		out := 0
		for _, epEvent := range p.epEvents[:n] {
			fd := int(epEvent.Fd)
			if fd == p.wake.readFD {
				p.wake.drain()
				continue
			}
			if out >= len(events) {
				break
			}
			events[out] = pollEvent{
				fd:       fd,
				readable: epEvent.Events&(syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLHUP|syscall.EPOLLERR) != 0,
				writable: epEvent.Events&syscall.EPOLLOUT != 0,
			}
			out++
		}

		return out, nil
	}
}

// Wake writes to the wakeup pipe so a blocked Wait returns.
func (p *epollPoller) Wake() error {
	return p.wake.wake()
}

// Close releases the wakeup pipe and the epoll descriptor.
func (p *epollPoller) Close() error {
	p.wake.close()
	return syscall.Close(p.epfd)
}

// enableKeepalive turns on TCP keepalive for an accepted socket, probing after
// eventLoopKeepaliveSeconds of idleness and then at the same interval.
func enableKeepalive(fd int) error {
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1); err != nil {
		return fmt.Errorf("SO_KEEPALIVE: %w", err)
	}
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, eventLoopKeepaliveSeconds); err != nil {
		return fmt.Errorf("TCP_KEEPIDLE: %w", err)
	}
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, eventLoopKeepaliveSeconds); err != nil {
		return fmt.Errorf("TCP_KEEPINTVL: %w", err)
	}
	return nil
}
