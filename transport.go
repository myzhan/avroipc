package avroipc

import (
	"io"
	"time"
)

type Transport interface {
	io.ReadWriteCloser

	Open() error
	Flush() error
	SetDeadline(t time.Time) error
}
