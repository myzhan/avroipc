package avroipc

import (
	"bytes"
	"encoding/binary"
	"log"
	"net"

	"github.com/linkedin/goavro"
)

// Client acts as an avro client
type Client struct {
	addr          string
	serial        int64
	connection    *net.TCPConn
	handshakeDone bool

	clientProtocol string
	serverHash     []byte
	clientHash     []byte
}

// NewClient creates an avro Client, and connect to addr immediately
func NewClient(addr string) *Client {

	client := &Client{
		addr: addr,
	}

	client.clientProtocol = messageProtocol
	client.clientHash = getMD5(client.clientProtocol)
	client.serverHash = getMD5(client.clientProtocol)

	client.connect()

	return client
}

func (client *Client) connect() {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", client.addr)
	var err error
	client.connection, err = net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Fatalf("%v", err)
	}

	// disable Nagle's algorithm
	client.connection.SetNoDelay(true)

	// first connect, need handshake
	client.handshake()
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

func (client *Client) getHandshakeRequest() []byte {

	handShakeMap := make(map[string]interface{})
	handShakeMap["clientHash"] = client.clientHash
	handShakeMap["serverHash"] = client.serverHash

	if client.clientProtocol != "" {
		protocolMap := make(map[string]interface{})
		protocolMap["string"] = client.clientProtocol
		handShakeMap["clientProtocol"] = protocolMap
	} else {
		handShakeMap["clientProtocol"] = nil
	}

	handShakeMap["meta"] = nil

	handShakeReq, err := handshakeRequestCodec.BinaryFromNative(nil, handShakeMap)
	if err != nil {
		log.Fatalf("%v", err)
	}
	return handShakeReq
}

func (client *Client) handshake() {

	handShakeReq := client.getHandshakeRequest()
	// a handshake ping with empty metadata and bogus message name
	handShakeReq = append(handShakeReq, 0, 0)

	responses := client.sendFrames(handShakeReq)
	handShakeResponse, _, err := handshakeResponseCodec.NativeFromBinary(responses[0])
	if err != nil {
		log.Fatalf("%v", err)
	}

	match := handShakeResponse.(map[string]interface{})["match"]
	switch match {
	case "NONE":
		// match=BOTH, serverProtocol=null, serverHash=null if the Client sent the valid hash of the server's protocol
		// and the server knows what protocol corresponds to the Client's hash. In this case, the request is complete
		// and the response data immediately follows the HandshakeResponse.
		serverProtocol := handShakeResponse.(map[string]interface{})["serverProtocol"].(map[string]interface{})["string"]
		serverHash := handShakeResponse.(map[string]interface{})["serverHash"].(map[string]interface{})["org.apache.avro.ipc.MD5"]
		client.clientProtocol = serverProtocol.(string)
		client.serverHash = serverHash.([]byte)
		log.Println("Protocol mismatched, re-handshake with server's protocol and server hash")
		client.handshake()
	case "CLIENT":
		// match=CLIENT, serverProtocol!=null, serverHash!=null if the server has previously seen the Client's protocol,
		// but the Client sent an incorrect hash of the server's protocol. The request is complete and the response data
		// immediately follows the HandshakeResponse. The Client must use the returned protocol to process the response
		// and should also cache that protocol and its hash for future interactions with this server.
		serverProtocol := handShakeResponse.(map[string]interface{})["serverProtocol"].(map[string]interface{})["string"]
		serverHash := handShakeResponse.(map[string]interface{})["serverHash"].(map[string]interface{})["org.apache.avro.ipc.MD5"]
		client.clientProtocol = serverProtocol.(string)
		client.serverHash = serverHash.([]byte)
		client.handshakeDone = true
	case "BOTH":
		// match=NONE if the server has not previously seen the Client's protocol. The serverHash and serverProtocol may
		// also be non-null if the server's protocol hash was incorrect. In this case the Client must then re-submit its
		// request with its protocol text (clientHash!=null, clientProtocol!=null, serverHash!=null) and the server
		// should respond with a successful match (match=BOTH, serverProtocol=null, serverHash=null) as above.
		client.handshakeDone = true
	}

}

// Append sends event to flume
func (client *Client) Append(event *Event) {
	messageHeader := messageHeader()
	payload := event.Bytes()
	client.sendFrames(messageHeader, payload)
}

// Codec is stateless and is safe to use by multiple go routines.
var handshakeRequestCodec *goavro.Codec
var handshakeResponseCodec *goavro.Codec
var eventCodec *goavro.Codec
var metaCodec *goavro.Codec

func init() {
	handshakeRequestCodec, _ = goavro.NewCodec(handshakeRequestProtocol)
	handshakeResponseCodec, _ = goavro.NewCodec(handshakeResponseProtocol)
	eventCodec, _ = goavro.NewCodec(eventProtocol)
	metaCodec, _ = goavro.NewCodec(metaProtocol)
}
