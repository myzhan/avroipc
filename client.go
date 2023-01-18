package avroipc

import (
	"fmt"
	"time"

	"github.com/myzhan/avroipc/layers"
	"github.com/myzhan/avroipc/protocols"
	"github.com/myzhan/avroipc/transports"
)

// An avro client implementation
type Client interface {
	Close() error
	SendMessage(method string, datum interface{}) (string, error)
}

type client struct {
	sendTimeout time.Duration

	transport         transports.Transport
	framingLayer      layers.FramingLayer
	callProtocol      protocols.CallProtocol
	handshakeProtocol protocols.HandshakeProtocol
}

// NewClient creates an avro client with considering values of options from
// the passed configuration object and connects to the specified remote Flume
// endpoint immediately.
//
// This constructor supposed to be used in production environments.
func NewClientWithConfig(addr string, proto protocols.MessageProtocol, config *Config) (Client, error) {
	c := &client{}
	c.sendTimeout = config.SendTimeout

	err := c.initTransports(addr, config)
	if err != nil {
		return nil, err
	}

	c.initProtocols(proto)
	return c, c.handshake()
}

func (c *client) initProtocols(proto protocols.MessageProtocol) {
	// All errors here are only related to compilations of Avro schemas
	// and are not possible at runtime because they will be caught by unit tests.
	c.framingLayer = layers.NewFraming(c.transport)
	c.callProtocol, _ = protocols.NewCall(proto)
	c.handshakeProtocol, _ = protocols.NewHandshake(proto)
}

func (c *client) initTransports(addr string, config *Config) (err error) {

	c.transport, err = transports.NewSocket(addr, config.Timeout)
	if err != nil {
		return err
	}

	if config.CompressionLevel > 0 {
		c.transport, err = transports.NewZlib(c.transport, config.CompressionLevel)
		if err != nil {
			return err
		}
	}

	if config.TLSConfig != nil {
		c.transport, err = transports.NewTLS(c.transport, config.TLSConfig)
		if err != nil {
			return err
		}
	}

	if config.BufferSize > 0 {
		c.transport = transports.NewBuffered(c.transport, config.BufferSize)
	}

	return
}

func (c *client) send(request []byte) ([]byte, error) {
	err := c.applyDeadline()
	if err != nil {
		return nil, err
	}

	err = c.framingLayer.Write(request)
	if err != nil {
		return nil, err
	}

	err = c.transport.Flush()
	if err != nil {
		return nil, err
	}

	response, err := c.framingLayer.Read()
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (c *client) handshake() error {
	request, err := c.handshakeProtocol.PrepareRequest()
	if err != nil {
		return err
	}

	responseBytes, err := c.send(request)
	if err != nil {
		return err
	}

	needResend, err := c.handshakeProtocol.ProcessResponse(responseBytes)
	if err != nil {
		return err
	}
	if needResend {
		err = c.handshake()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *client) applyDeadline() error {
	if c.sendTimeout > 0 {
		d := time.Now().Add(c.sendTimeout)
		return c.transport.SetDeadline(d)
	}

	return nil
}

func (c *client) Close() error {
	err := c.applyDeadline()
	if err != nil {
		return err
	}

	return c.transport.Close()
}

func (c *client) SendMessage(method string, datum interface{}) (string, error) {
	request, err := c.callProtocol.PrepareRequest(method, datum)
	if err != nil {
		return "", err
	}

	responseBytes, err := c.send(request)
	if err != nil {
		return "", err
	}

	response, err := c.callProtocol.ParseResponse(method, responseBytes)
	if err != nil {
		return "", err
	}

	status, ok := response.(string)
	if !ok {
		return "", fmt.Errorf("cannot convert status to string: %v", response)
	}

	return status, nil
}
