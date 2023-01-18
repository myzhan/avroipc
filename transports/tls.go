package transports

import "crypto/tls"

type TLS struct {
	*tls.Conn
	trans Transport
}

var _ Transport = new(TLS)

func NewTLS(trans Transport, tlsConfig *tls.Config) (*TLS, error) {

	conn := tls.Client(trans, tlsConfig)

	if err := conn.Handshake(); err != nil {
		return nil, err
	}

	return &TLS{
		Conn:  conn,
		trans: trans,
	}, nil
}

func (t *TLS) Flush() error {
	return t.trans.Flush()
}
