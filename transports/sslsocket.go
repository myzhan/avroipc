package transports

import (
	"compress/zlib"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"
)

type sslsocket struct {
	net.Conn
}

type zlibConn struct {
	rawConn          net.Conn
	compressionLevel int

	zw *zlib.Writer
	zr io.ReadCloser
}

func newZlibConn(rawConn net.Conn, compressionLevel int) (*zlibConn, error) {
	conn := &zlibConn{
		rawConn:          rawConn,
		compressionLevel: compressionLevel,
	}

	return conn, nil
}

var _ net.Conn = new(zlibConn)

func (c *zlibConn) Read(b []byte) (n int, err error) {

	if c.zr == nil {
		c.zr, err = zlib.NewReader(c.rawConn)
		if err != nil {
			return 0, fmt.Errorf("zlibconn.read. zlib.NewReader. %w", err)
		}
	}

	return c.zr.Read(b)
}

func (c *zlibConn) Write(b []byte) (n int, err error) {

	if c.zw == nil {
		c.zw, err = zlib.NewWriterLevel(c.rawConn, c.compressionLevel)
		if err != nil {
			return 0, fmt.Errorf("zlibconn.write. zlib.NewWriterLevel. %w", err)
		}
	}

	n, err = c.zw.Write(b)
	if err != nil {
		return 0, fmt.Errorf("zlibconn.write err: %w", err)
	}

	return n, c.zw.Flush()
}

func (c *zlibConn) Close() error {
	return c.rawConn.Close()
}

func (c *zlibConn) LocalAddr() net.Addr {
	return c.rawConn.LocalAddr()
}

func (c *zlibConn) RemoteAddr() net.Addr {
	return c.rawConn.RemoteAddr()
}

func (c *zlibConn) SetDeadline(t time.Time) error {
	return c.rawConn.SetDeadline(t)
}

func (c *zlibConn) SetReadDeadline(t time.Time) error {
	return c.rawConn.SetReadDeadline(t)
}

func (c *zlibConn) SetWriteDeadline(t time.Time) error {
	return c.rawConn.SetWriteDeadline(t)
}

var _ Transport = new(sslsocket)

func NewSSLSocket(hostPort string, tlsConfig *tls.Config, compressionLevel int) (Transport, error) {

	addr, err := net.ResolveTCPAddr("tcp", hostPort)
	if err != nil {
		return nil, err
	}

	s := &sslsocket{}

	if compressionLevel > 0 {
		rawConn, err := net.Dial(addr.Network(), addr.String())
		if err != nil {
			return nil, err
		}

		zlibConn, err := newZlibConn(rawConn, compressionLevel)
		if err != nil {
			return nil, err
		}

		conn := tls.Client(zlibConn, tlsConfig)
		if err != nil {
			return nil, err
		}

		err = conn.Handshake()
		if err != nil {
			return nil, err
		}

		s.Conn = conn
	} else {
		s.Conn, err = tls.Dial(addr.Network(), addr.String(), tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("TLS connection failed: %w", err)
		}
	}

	return s, nil
}

func (s *sslsocket) Flush() error {
	return nil
}
