package avroipc

import (
	"bytes"
	"encoding/binary"
	"github.com/linkedin/goavro"
)

// Client acts as an avro client
type Client struct {
	serial int64

	connection        Transport
	handshakeProtocol HandshakeProtocol
}

// NewClient creates an avro Client, and connect to addr immediately
func NewClient(addr string) (client *Client, err error) {
	client = &Client{}

	client.connection, err = NewSocket(addr)
	if err != nil {
		return nil, err
	}

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

func (client *Client) sendFrames(requests ...[]byte) [][]byte {

	// incr serial
	client.serial = client.serial + 1

	payload := new(bytes.Buffer)

	// write header
	binary.Write(payload, binary.BigEndian, int32(client.serial))
	binary.Write(payload, binary.BigEndian, int32(len(requests)))

	for i := 0; i < len(requests); i++ {
		// write body header
		binary.Write(payload, binary.BigEndian, int32(len(requests[i])))
		// write body
		payload.Write(requests[i])
	}

	// send request
	client.connection.Write(payload.Bytes())

	// read header
	serial := recvBytes(client.connection, 4)
	_ = binary.BigEndian.Uint32(serial)

	size := recvBytes(client.connection, 4)
	sizeValue := binary.BigEndian.Uint32(size)

	response := make([][]byte, sizeValue)

	for i := 0; i < int(sizeValue); i++ {
		// read header
		length := recvBytes(client.connection, 4)
		lengthValue := binary.BigEndian.Uint32(length)
		body := recvBytes(client.connection, int(lengthValue))
		response[i] = body
	}

	return response
}

func (client *Client) handshake() (err error) {
	request, err := client.handshakeProtocol.PrepareRequest()
	if err != nil {
		return err
	}

	responseBytes := client.sendFrames(request)

	needResend, err := client.handshakeProtocol.ProcessResponse(responseBytes[0])
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
func (client *Client) Append(event *Event) {
	messageHeader := messageHeader()
	payload := event.Bytes()
	client.sendFrames(messageHeader, payload)
}

// Codec is stateless and is safe to use by multiple go routines.
var eventCodec *goavro.Codec

func init() {
	eventCodec, _ = goavro.NewCodec(eventSchema)
}
