package transports

import (
	"compress/zlib"
	"io"
)

type zlibTransport struct {
	r io.ReadCloser
	w *zlib.Writer
	Transport
}

func NewZlib(trans Transport, level int) (Transport, error) {
	w, err := zlib.NewWriterLevel(trans, level)
	if err != nil {
		return nil, err
	}

	return &zlibTransport{
		w:         w,
		Transport: trans,
	}, nil
}

func (t *zlibTransport) Close() error {
	if t.r != nil {
		err := t.r.Close()
		if err != nil {
			return err
		}
	}

	err := t.w.Close()
	if err != nil {
		return err
	}

	return t.Transport.Close()
}

func (t *zlibTransport) Read(p []byte) (int, error) {
	// Use lazy initialization of a reader because it immediately starts reading a header
	// so may hang if there is no data in the underlying transport
	if t.r == nil {
		r, err := zlib.NewReader(t.Transport)
		if err != nil {
			return 0, err
		}
		t.r = r
	}

	return t.r.Read(p)
}

func (t *zlibTransport) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	if err != nil {
		return 0, err
	}

	return n, t.w.Flush()
}

func (t *zlibTransport) Flush() error {
	/*
		err := t.w.Flush()
		if err != nil {
			return err
		}
	*/

	return t.Transport.Flush()
}
