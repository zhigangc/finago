package finago

import "net"
import "math/rand"
import "sync"
import "time"

type destEntry struct {
	dest           string
	numConnCreated int
	numConnUsed    int
	numPending     int
	connQueue      chan net.Conn
	rwLock         *sync.RWMutex
	connectTimeout time.Duration
}

func (entry *destEntry) getConn() (net.Conn, error) {
	select {
	case conn := <-entry.connQueue:
		return conn, nil
	default:
		//create a new connection next
	}

	conn, err := net.DialTimeout("tcp", entry.dest, entry.connectTimeout)
	if err != nil {
		return nil, err
	}
	entry.rwLock.Lock()
	entry.numConnCreated += 1
	entry.numConnUsed += 1
	entry.rwLock.Unlock()
	return conn, nil
}

func (entry *destEntry) free(conn net.Conn) error {
	entry.connQueue <- conn
	entry.rwLock.Lock()
	entry.numConnUsed -= 1
	entry.rwLock.Unlock()
	return nil
}

type ConnectionPoolFactory struct {
	entries []*destEntry
}

type ConnectionPool struct {
	conn    net.Conn
	dEntry  *destEntry
	factory *ConnectionPoolFactory
}

func NewConnectionPoolFactory(
	dests []string,
	maxConnections int,
	connectTimeout time.Duration) *ConnectionPoolFactory {
	entries := make([]*destEntry, 0, len(dests))
	for _, dest := range dests {
		entry := &destEntry{
			dest:           dest,
			connQueue:      make(chan net.Conn, maxConnections),
			rwLock:         new(sync.RWMutex),
			connectTimeout: connectTimeout,
		}
		entries = append(entries, entry)
	}
	return &ConnectionPoolFactory{entries: entries}
}

func (factory *ConnectionPoolFactory) Build() *ConnectionPool {
	return &ConnectionPool{conn: nil, factory: factory}
}

func (factory *ConnectionPoolFactory) choose() *destEntry {
	size := len(factory.entries)
	choice := rand.Intn(size)
	dEntry := factory.entries[choice]
	return dEntry
}

func (cp *ConnectionPool) getConn() (net.Conn, error) {
	if conn := cp.conn; conn != nil {
		return conn, nil
	}
	dEntry := cp.factory.choose()
	conn, err := dEntry.getConn()
	if err != nil {
		return nil, err
	}
	cp.conn = conn
	cp.dEntry = dEntry
	return conn, nil
}

func (cp *ConnectionPool) Read(b []byte) (n int, err error) {
	conn, err := cp.getConn()
	if err != nil {
		return 0, err
	}
	return conn.Read(b)
}

// Write writes data to the connection.
// Write can be made to time out and return a Error with Timeout() == true
// after a fixed time limit; see SetDeadline and SetWriteDeadline.
func (cp *ConnectionPool) Write(b []byte) (n int, err error) {
	conn, err := cp.getConn()
	if err != nil {
		return 0, err
	}
	return conn.Write(b)
}

// Close closes the connection.
// Any blocked Read or Write operations will be unblocked and return errors.
func (cp *ConnectionPool) Close() error {
	if cp.conn != nil && cp.dEntry != nil {
		cp.dEntry.free(cp.conn)
		cp.dEntry = nil
		cp.conn = nil
	} else if cp.conn != nil {
		cp.conn.Close()
		cp.conn = nil
	}
	return nil
}

// LocalAddr returns the local network address.
func (cp *ConnectionPool) LocalAddr() net.Addr {
	conn, err := cp.getConn()
	if err != nil {
		return nil
	}
	return conn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (cp *ConnectionPool) RemoteAddr() net.Addr {
	conn, err := cp.getConn()
	if err != nil {
		return nil
	}
	return conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines associated
// with the connection. It is equivalent to calling both
// SetReadDeadline and SetWriteDeadline.
//
// A deadline is an absolute time after which I/O operations
// fail with a timeout (see type Error) instead of
// blocking. The deadline applies to all future I/O, not just
// the immediately following call to Read or Write.
//
// An idle timeout can be implemented by repeatedly extending
// the deadline after successful Read or Write calls.
//
// A zero value for t means I/O operations will not time out.
func (cp *ConnectionPool) SetDeadline(t time.Time) error {
	conn, err := cp.getConn()
	if err != nil {
		return err
	}
	return conn.SetDeadline(t)
}

// SetReadDeadline sets the deadline for future Read calls.
// A zero value for t means Read will not time out.
func (cp *ConnectionPool) SetReadDeadline(t time.Time) error {
	conn, err := cp.getConn()
	if err != nil {
		return err
	}
	return conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future Write calls.
// Even if write times out, it may return n > 0, indicating that
// some of the data was successfully written.
// A zero value for t means Write will not time out.
func (cp *ConnectionPool) SetWriteDeadline(t time.Time) error {
	conn, err := cp.getConn()
	if err != nil {
		return err
	}
	return conn.SetWriteDeadline(t)
}
