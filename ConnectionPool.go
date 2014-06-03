package finago

//import "fmt"
import "net"
import "math/rand"
import "time"

var connMap map[int]net.Conn

func init() {
	connMap = make(map[int]net.Conn)
}

type ConnectionPool struct {
	connId int
	dests  []string
}

func NewConnectionPool(dests []string) *ConnectionPool {
	return &ConnectionPool{connId: -1, dests: dests}
}

func (cp *ConnectionPool) selectDest() string {
	size := len(cp.dests)
	choice := rand.Intn(size)
	return cp.dests[choice]
}

func (cp *ConnectionPool) getConn() net.Conn {
	if connId := cp.connId; connId >= 0 {
		return connMap[connId]
	}
	addr := cp.selectDest()
	conn, _ := net.DialTimeout("tcp", addr, 10)
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
	return cp.getConn().Close()
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
