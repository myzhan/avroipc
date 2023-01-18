package transports

import (
	"net"
)

type Transport interface {
	net.Conn

	Flush() error
}
