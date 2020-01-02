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
	Append(event *Event) (string, error)
	AppendBatch(events []*Event) (string, error)
}

type client struct {
	sendTimeout time.Duration

	transport         transports.Transport
	framingLayer      layers.FramingLayer
	callProtocol      protocols.CallProtocol
	handshakeProtocol protocols.HandshakeProtocol
}

// NewClient creates an avro client with default option values and
// connects to the specified remote Flume endpoint immediately.
//
// Very useful for the testing purposes and to build simple examples.
func NewClient(addr string) (Client, error) {
	return NewClientWithConfig(addr, NewConfig())
}

// NewClient creates an avro client with considering values of options from
// the passed configuration object and connects to the specified remote Flume
// endpoint immediately.
//
// This constructor supposed to be used in production environments.
func NewClientWithConfig(addr string, config *Config) (Client, error) {
	c := &client{}
	c.sendTimeout = config.SendTimeout

	err := c.initTransports(addr, config)
	if err != nil {
		return nil, err
	}

	err = c.initProtocols()
	if err != nil {
		return nil, err
	}

	return c, c.handshake()
}

func (c *client) initProtocols() error {
	proto, err := protocols.NewAvroSource()
	if err != nil {
		return err
	}

	c.framingLayer = layers.NewFraming(c.transport)
	c.callProtocol, err = protocols.NewCall(proto)
	if err != nil {
		return err
	}
	c.handshakeProtocol, err = protocols.NewHandshake()
	if err != nil {
		return err
	}
	return nil
}

func (c *client) initTransports(addr string, config *Config) (err error) {
	c.transport, err = transports.NewSocketTimeout(addr, config.Timeout)
	if err != nil {
		return err
	}

	if config.BufferSize > 0 {
		c.transport = transports.NewBuffered(c.transport, config.BufferSize)
	}
	if config.CompressionLevel > 0 {
		c.transport, err = transports.NewZlib(c.transport, config.CompressionLevel)
		if err != nil {
			return err
		}
	}

	return c.transport.Open()
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

func (c *client) sendMessage(method string, datum interface{}) (string, error) {
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

func (c *client) applyDeadline() error {
	if c.sendTimeout > 0 {
		d := time.Now().Add(c.sendTimeout)
		return c.transport.SetDeadline(d)
	}

	return nil
}

// Append sends event to flume
func (c *client) Append(event *Event) (string, error) {
	datum := event.toMap()

	return c.sendMessage("append", datum)
}

// Append sends events to flume
func (c *client) AppendBatch(events []*Event) (string, error) {
	datum := make([]map[string]interface{}, 0)
	for _, event := range events {
		datum = append(datum, event.toMap())
	}

	return c.sendMessage("appendBatch", datum)
}

func (c *client) Close() error {
	err := c.applyDeadline()
	if err != nil {
		return err
	}

	return c.transport.Close()
}
