package protocols

import (
	"bytes"
	"crypto/md5"
	"fmt"

	"github.com/linkedin/goavro"
	"github.com/sirupsen/logrus"
)

func getMD5(str string) []byte {
	sum := md5.Sum([]byte(str))
	return sum[:]
}

type HandshakeProtocol interface {
	PrepareRequest() ([]byte, error)
	ProcessResponse(responseBytes []byte) (bool, error)
}

// The Avro Handshake implementation for the Avro RPC protocol.
//
// It is used for establishing a stateful connection between a client and a server.
//
// See http://avro.apache.org/docs/1.8.2/spec.html#handshake for details.
type handshakeProtocol struct {
	logger *logrus.Entry

	serverHash     []byte
	clientHash     []byte
	clientProtocol string

	needClientProtocol bool

	handshakeRequestCodec  *goavro.Codec
	handshakeResponseCodec *goavro.Codec
}

func NewHandshake() (HandshakeProtocol, error) {
	p := &handshakeProtocol{
		serverHash:     getMD5(messageProtocol),
		clientHash:     getMD5(messageProtocol),
		clientProtocol: messageProtocol,
	}

	p.logger = logrus.WithFields(logrus.Fields{
		"name": "AvroHandshakeProtocol",
	})
	p.logger.Debug("created")

	err := p.init()
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *handshakeProtocol) init() (err error) {
	p.handshakeRequestCodec, err = goavro.NewCodec(handshakeRequestSchema)
	if err != nil {
		return
	}
	p.handshakeResponseCodec, err = goavro.NewCodec(handshakeResponseSchema)
	if err != nil {
		return
	}

	return
}

func (p *handshakeProtocol) PrepareRequest() ([]byte, error) {
	request := make(map[string]interface{})

	request["meta"] = nil
	request["clientHash"] = p.clientHash
	request["serverHash"] = p.serverHash

	if !p.needClientProtocol {
		request["clientProtocol"] = nil
	} else {
		request["clientProtocol"] = map[string]interface{}{
			"string": p.clientProtocol,
		}
	}

	requestBytes, err := p.handshakeRequestCodec.BinaryFromNative(nil, request)
	if err != nil {
		return nil, err
	}

	emptyMessage := []byte{0, 0}

	buf := bytes.NewBuffer(requestBytes)
	buf.Write(emptyMessage)

	return buf.Bytes(), nil
}

func (p *handshakeProtocol) ProcessResponse(responseBytes []byte) (bool, error) {
	response, _, err := p.handshakeResponseCodec.NativeFromBinary(responseBytes)
	if err != nil {
		return false, err
	}

	responseMap, ok := response.(map[string]interface{})
	if !ok {
		return false, fmt.Errorf("cannot convert handshake response: %v", responseMap)
	}

	match := responseMap["match"]
	serverHash := responseMap["serverHash"]
	serverProtocol := responseMap["serverProtocol"]
	switch match {
	case "BOTH":
		p.logger.Debug("handshake is successful")

		if serverHash != nil {
			p.logger.Warn("unexpected server's hash")
		}
		if serverProtocol != nil {
			p.logger.Warn("unexpected server's protocol")
		}
	case "NONE":
		p.logger.Debug("unknown client's protocol")

		err := p.setServerHash(serverHash)
		if err != nil {
			return false, err
		}

		if p.needClientProtocol {
			return false, fmt.Errorf("handshake failed: unknown client's protocol")
		} else {
			p.needClientProtocol = true
		}

		return true, nil
	case "CLIENT":
		p.logger.Debug("update server's protocol")

		if serverHash == nil {
			p.logger.Warn("expected server's hash but got nil")
		}
		if serverProtocol == nil {
			p.logger.Warn("expected server's protocol but got nil")
		}

		if p.needClientProtocol {
			return false, fmt.Errorf("handshake failed: unknown client's protocol")
		}

		err := p.setServerHash(serverHash)
		if err != nil {
			return false, err
		}
	default:
		return false, fmt.Errorf("unknown handshake response match field: %v", match)
	}

	return false, nil
}

func (p *handshakeProtocol) setServerHash(serverHash interface{}) error {
	if serverHash == nil {
		return nil
	}

	serverHashMap, ok := serverHash.(map[string]interface{})
	if !ok {
		return fmt.Errorf("cannot convert hash to map: %v", serverHashMap)
	}

	md5Int, ok := serverHashMap["org.apache.avro.ipc.MD5"]
	if !ok {
		return fmt.Errorf("MD5 not found in map: %v", serverHashMap)
	}

	md5Bytes, ok := md5Int.([]byte)
	if !ok {
		return fmt.Errorf("cannot convert MD5 to byte array: %v", md5Int)
	}

	p.serverHash = md5Bytes

	return nil
}
