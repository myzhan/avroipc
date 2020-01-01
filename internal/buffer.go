package internal

import (
	"io"
)

type Buffer struct {
	buf []byte
}

func (b *Buffer) Len() int {
	return len(b.buf)
}

func (b *Buffer) Bytes() []byte {
	return b.buf
}

func (b *Buffer) Reset() {
	b.buf = b.buf[:0]
}

func (b *Buffer) ReadFrom(r io.Reader) error {
	buf := make([]byte, 1024)

	for {
		n, err := r.Read(buf)
		if err != nil {
			return err
		}
		b.buf = append(b.buf, buf[:n]...)
		if n < len(buf) {
			break
		}
	}

	return nil
}
