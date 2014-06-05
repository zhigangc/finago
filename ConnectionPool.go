package finago

//import "fmt"
import "net"
import "math/rand"
import "sync"
import "time"

const MaxConnections = 8

type destEntry struct {
	dest           string
	numConnCreated int
	numConnUsed    int
	numPending     int
	connQueue      chan net.Conn
	rwLock         *sync.RWMutex
}

func (entry *destEntry) getConn() (net.Conn, error) {
	if conn := <-entry.connQueue; conn != nil {
		return conn, nil
	}

	conn, err := net.DialTimeout("tcp", entry.dest, 10)
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

func NewConnectionPoolFactory(dests []string) *ConnectionPoolFactory {
	entries := make([]*destEntry, 0, len(dests))
	for _, dest := range dests {
		connQueue := make(chan net.Conn, MaxConnections)
		for i := 0; i < MaxConnections; i++ {
			connQueue <- nil
		}
		entry := &destEntry{
			dest:      dest,
			connQueue: connQueue,
			rwLock:    new(sync.RWMutex),
		}
		entries = append(entries, entry)
	}
	return &ConnectionPoolFactory{entries: entries}
}

func (factory *ConnectionPoolFactory) Build() *ConnectionPool {
	return &ConnectionPool{conn: nil, factory: factory}
}

func (factory *ConnectionPoolFactory) freeConn(conn net.Conn) {

}

func (factory *ConnectionPoolFactory) choose() *destEntry {
	size := len(factory.entries)
	choice := rand.Intn(size)
	dEntry := factory.entries[choice]
	return dEntry
}

func (factory *ConnectionPoolFactory) getDest() *destEntry {
	return factory.choose()
}

func (cp *ConnectionPool) getConn() net.Conn {
	if conn := cp.conn; conn != nil {
		return conn
	}
	dEntry := cp.factory.getDest()
	conn, _ := dEntry.getConn()
	cp.conn = conn
	cp.dEntry = dEntry
	return conn
}

func (cp *ConnectionPool) Read(b []byte) (n int, err error) {
	return cp.getConn().Read(b)
}

// Write writes data to the connection.
// Write can be made to time out and return a Error with Timeout() == true
// after a fixed time limit; see SetDeadline and SetWriteDeadline.
func (cp *ConnectionPool) Write(b []byte) (n int, err error) {
	return cp.getConn().Write(b)
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
	return cp.getConn().LocalAddr()
}

// RemoteAddr returns the remote network address.
func (cp *ConnectionPool) RemoteAddr() net.Addr {
	return cp.getConn().RemoteAddr()
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
	return cp.getConn().SetDeadline(t)
}

// SetReadDeadline sets the deadline for future Read calls.
// A zero value for t means Read will not time out.
func (cp *ConnectionPool) SetReadDeadline(t time.Time) error {
	return cp.getConn().SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future Write calls.
// Even if write times out, it may return n > 0, indicating that
// some of the data was successfully written.
// A zero value for t means Write will not time out.
func (cp *ConnectionPool) SetWriteDeadline(t time.Time) error {
	return cp.getConn().SetWriteDeadline(t)
}
