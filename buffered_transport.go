package avroipc

import (
	"bufio"
)

type bufferedTransport struct {
	r     *bufio.Reader
	w     *bufio.Writer
	trans Transport
}

func NewBufferedTransport(trans Transport, bufferSize int) Transport {
	return &bufferedTransport{
		r:     bufio.NewReaderSize(trans, bufferSize),
		w:     bufio.NewWriterSize(trans, bufferSize),
		trans: trans,
	}
}

func (p *bufferedTransport) Open() (err error) {
	return p.trans.Open()
}

func (p *bufferedTransport) Close() (err error) {
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
