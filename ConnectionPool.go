package finago

//import "fmt"
import "net"
import "math/rand"
import "sync"
import "time"

type connEntry struct {
	conn net.Conn
	waitQueue int
}

type destEntry struct {
	dest string
	rwlock *sync.RWMutex
}

func (entry *destEntry) getConn() net.Conn {
	var conn net.Conn
	entry.rwlock.Lock()
	defer entry.rwlock.Unlock()
	if len(entry.free) > 0 {
		var front *connEntry
		front, entry.free = entry.free[0], entry.free[1:]
		front.state = 1
		entry.used = append(entry.used, front)
		conn = front.conn
	} else {
		conn, _ = net.DialTimeout("tcp", entry.dest, 10)
		host, port, _ := net.SplitHostPort(entry.dest)
		cEntry := &connEntry{
			conn: conn,
			host: host,
			port: port,
			state: 0,
		}
		entry.used = append(entry.used, cEntry)
	}
	return conn
} 

type ConnectionPoolFactory struct {
	entries []*destEntry
}

type ConnectionPool struct {
	conn net.Conn
	factory *ConnectionPoolFactory
}

func NewConnectionPoolFactory(dests []string) *ConnectionPoolFactory {
	entries := make([]*destEntry, 0, len(dests))
	for _, dest := range dests {
		entry := &destEntry {
			dest: dest,
			rwlock: new(sync.RWMutex)
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
	dest := factory.dests[choice]
	return dest
}

func (factory *ConnectionPoolFactory) getConn() net.Conn {
	dest := factory.chooseDest()
	if dEntry, ok := factory.destMap[dest]; ok {
		return dEntry.getConn()
	}
	dEntry := &destEntry {
		dest: dest,
		free: nil,
		used: nil,
		rwlock: new(sync.RWMutex),
	}
	factory.destMap[dest] = dEntry
	return dEntry.getConn()
}

func (cp *ConnectionPool) getConn() net.Conn {
	if conn:= cp.conn; conn != nil {
		return conn
	}
	conn := cp.factory.getConn()
	cp.conn = conn
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
	if cp.conn != nil {
		cp.factory.freeConn(cp.conn)
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
