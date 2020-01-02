package transports

import (
	"io"
	"time"
)

type Transport interface {
	io.ReadWriteCloser

	Flush() error
	SetDeadline(t time.Time) error
}
