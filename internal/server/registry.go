package server

import (
	"errors"
	"net"
	"sync"
)

// ClientConn defines the operations the server tracks for each client. It can
// close and identify the connection. This narrower alternative to net.Conn
// prevents callers from depending on direct socket I/O or deadlines. The event
// loop drives its connections exclusively from one goroutine and does not
// expose those operations.
type ClientConn interface {
	Close() error
	RemoteAddr() net.Addr
}

// ClientRegistry tracks active client connections.
type ClientRegistry struct {
	mu      sync.RWMutex
	nextID  uint64
	clients map[uint64]ClientConn
}

// NewClientRegistry creates an empty client registry.
func NewClientRegistry() *ClientRegistry {
	return &ClientRegistry{clients: make(map[uint64]ClientConn)}
}

// Add registers a new client connection and returns its unique ID.
func (r *ClientRegistry) Add(conn ClientConn) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextID++
	r.clients[r.nextID] = conn
	return r.nextID
}

// Remove removes a client connection from the registry.
func (r *ClientRegistry) Remove(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.clients, id)
}

// Count returns the number of active client connections.
func (r *ClientRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.clients)
}

// CloseAll closes every tracked client connection.
func (r *ClientRegistry) CloseAll() error {
	r.mu.Lock()
	clients := make([]ClientConn, 0, len(r.clients))
	for id, conn := range r.clients {
		clients = append(clients, conn)
		delete(r.clients, id)
	}
	r.mu.Unlock()

	var closeErr error
	for _, conn := range clients {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErr = errors.Join(closeErr, err)
		}
	}

	return closeErr
}
