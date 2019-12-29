package avroipc

import (
	"fmt"
	"time"

	"github.com/myzhan/avroipc/layers"
	"github.com/myzhan/avroipc/protocols"
	"github.com/myzhan/avroipc/transports"
)

// Client acts as an avro client
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

// NewClient creates an avro Client, and connect to addr immediately
func NewClient(addr string, timeout, sendTimeout time.Duration, bufferSize, compressionLevel int) (Client, error) {
	trans, err := transports.NewSocketTimeout(addr, timeout)
	if err != nil {
		return nil, err
	}
	if bufferSize > 0 {
		trans = transports.NewBuffered(trans, bufferSize)
	}
	if compressionLevel > 0 {
		trans, err = transports.NewZlib(trans, compressionLevel)
		if err != nil {
			return nil, err
		}
	}
	err = trans.Open()
	if err != nil {
		return nil, err
	}

	proto, err := protocols.NewAvroSource()
	if err != nil {
		return nil, err
	}

	return NewClientWithTrans(trans, proto, sendTimeout)
}

func NewClientWithTrans(trans transports.Transport, proto protocols.MessageProtocol, sendTimeout time.Duration) (Client, error) {
	var err error
	c := &client{}
	c.sendTimeout = sendTimeout

	c.transport = trans
	c.framingLayer = layers.NewFraming(trans)

	c.callProtocol, err = protocols.NewCall(proto)
	if err != nil {
		return nil, err
	}

	c.handshakeProtocol, err = protocols.NewHandshake()
	if err != nil {
		return nil, err
	}

	err = c.handshake()
	if err != nil {
		return nil, err
	}

	return c, nil
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
