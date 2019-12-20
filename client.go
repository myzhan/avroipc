package avroipc

import (
	"fmt"
)

// Client acts as an avro client
type Client interface {
	Close() error
	Append(event *Event) (string, error)
	AppendBatch(event []*Event) (string, error)
}

type client struct {
	framingLayer      FramingLayer
	callProtocol      CallProtocol
	handshakeProtocol HandshakeProtocol
}

// NewClient creates an avro Client, and connect to addr immediately
func NewClient(addr string, compressionLevel int) (Client, error) {
	trans, err := NewSocket(addr)
	if err != nil {
		return nil, err
	}
	if compressionLevel > 0 {
		trans, err = NewZlibTransport(trans, compressionLevel)
		if err != nil {
			return nil, err
		}
	}
	err = trans.Open()
	if err != nil {
		return nil, err
	}

	proto, err := NewAvroSourceProtocol()
	if err != nil {
		return nil, err
	}

	return NewClientWithTrans(trans, proto)
}

func NewClientWithTrans(trans Transport, proto MessageProtocol) (Client, error) {
	c := &client{}
	var err error

	c.framingLayer = NewFramingLayer(trans)

	c.callProtocol, err = NewCallProtocol(proto)
	if err != nil {
		return nil, err
	}

	c.handshakeProtocol, err = NewHandshakeProtocol()
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
	err := c.framingLayer.Write(request)
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

	response, responseBytes, err := c.callProtocol.ParseResponse(method, responseBytes)
	if err != nil {
		return "", err
	}

	r := responseBytes
	n := len(responseBytes)
	if n > 0 {
		return "", fmt.Errorf("response buffer is not empty: len=%d, rest=0x%X", n, r)
	}

	status, ok := response.(string)
	if !ok {
		return "", fmt.Errorf("cannot convert status to string: %v", response)
	}

	return status, nil
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
	return c.framingLayer.Close()
}
