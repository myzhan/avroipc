package avroipc

import (
	"io"
)

type Transport interface {
	io.ReadWriteCloser

	Open() error
	Flush() error
}
