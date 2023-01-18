package transports

import (
	"bufio"
	"net"
	"time"
)

type bufferedTransport struct {
	r     *bufio.Reader
	w     *bufio.Writer
	trans Transport
}

func NewBuffered(trans Transport, bufferSize int) Transport {
	return &bufferedTransport{
		r:     bufio.NewReaderSize(trans, bufferSize),
		w:     bufio.NewWriterSize(trans, bufferSize),
		trans: trans,
	}
}

func (p *bufferedTransport) Close() error {
	err := p.w.Flush()
	if err != nil {
		return err
	}
	return p.trans.Close()
}

func (p *bufferedTransport) Read(b []byte) (int, error) {
	return p.r.Read(b)
}

func (p *bufferedTransport) Write(b []byte) (int, error) {
	return p.w.Write(b)
}

func (p *bufferedTransport) Flush() error {
	err := p.w.Flush()
	if err != nil {
		return err
	}
	return p.trans.Flush()
}

func (p *bufferedTransport) SetDeadline(t time.Time) error {
	return p.trans.SetDeadline(t)
}

func (p *bufferedTransport) LocalAddr() net.Addr {
	return p.trans.LocalAddr()
}

func (p *bufferedTransport) RemoteAddr() net.Addr {
	return p.trans.RemoteAddr()
}

func (p *bufferedTransport) SetReadDeadline(t time.Time) error {
	return p.trans.SetReadDeadline(t)
}

func (p *bufferedTransport) SetWriteDeadline(t time.Time) error {
	return p.trans.SetWriteDeadline(t)
}
