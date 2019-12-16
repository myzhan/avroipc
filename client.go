package avroipc

import (
	"bytes"
	"github.com/linkedin/goavro"
	"github.com/sirupsen/logrus"
)

// Client acts as an avro client
type Client struct {
	serial int64
	logger *logrus.Entry

	connection        Transport
	framingLayer      FramingLayer
	handshakeProtocol HandshakeProtocol
}

// NewClient creates an avro Client, and connect to addr immediately
func NewClient(addr string) (client *Client, err error) {
	client = &Client{}

	client.logger = logrus.WithFields(logrus.Fields{
		"name": "AvroFlumeClient",
	})
	client.logger.Debug("created")

	client.connection, err = NewSocket(addr)
	if err != nil {
		return nil, err
	}

	client.framingLayer = NewFramingLayer(client.connection)

	client.handshakeProtocol, err = NewHandshakeProtocol()
	if err != nil {
		return nil, err
	}

	err = client.connect()
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (client *Client) connect() (err error) {
	err = client.connection.Open()
	if err != nil {
		return err
	}

	// first connect, need handshake
	err = client.handshake()
	if err != nil {
		return err
	}

	return nil
}

func (client *Client) send(request []byte) ([]byte, error) {
	err := client.framingLayer.Write(request)
	if err != nil {
		return nil, err
	}
	client.logger.WithField("size", len(request)).Debug("sent request")

	response, err := client.framingLayer.Read()
	if err != nil {
		return nil, err
	}
	client.logger.WithField("size", len(response)).Debug("read response")

	return response, nil
}

func (client *Client) handshake() error {
	client.logger.Debug("start handshake")

	request, err := client.handshakeProtocol.PrepareRequest()
	if err != nil {
		return err
	}

	responseBytes, err := client.send(request)
	if err != nil {
		return err
	}

	needResend, err := client.handshakeProtocol.ProcessResponse(responseBytes)
	if err != nil {
		return err
	}
	if needResend {
		err = client.handshake()
		if err != nil {
			return err
		}
	}

	return nil
}

// Append sends event to flume
func (client *Client) Append(event *Event) error {
	messageHeader := messageHeader()
	payload := event.Bytes()

	buf := bytes.NewBuffer([]byte{})
	buf.Write(messageHeader)
	buf.Write(payload)
	responseBytes, err := client.send(buf.Bytes())
	if err != nil {
		return err
	}
	_ = responseBytes
	return nil
}

// Codec is stateless and is safe to use by multiple go routines.
var eventCodec *goavro.Codec

func init() {
	eventCodec, _ = goavro.NewCodec(eventSchema)
}
