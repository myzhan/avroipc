package avroipc_test

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/linkedin/goavro"
	"github.com/stretchr/testify/require"

	"github.com/myzhan/avroipc"
	"github.com/myzhan/avroipc/internal"
)

func TestReal(t *testing.T) {
	addr := "127.0.0.1:20201"
	bufferSize := 8
	compressionLevel := 0

	timeout := time.Duration(0)
	sendTimeout := time.Duration(0)

	client, err := avroipc.NewClient(addr, timeout, sendTimeout, bufferSize, compressionLevel)
	require.NoError(t, err)

	event := &avroipc.Event{
		Body: []byte("tttt"),
	}

	status, err := client.Append(event)
	require.NoError(t, err)
	require.Equal(t, "OK", status)

	// Close the client finally.
	require.NoError(t, client.Close())
}

func TestDecode(t *testing.T) {
	b := []byte{
		0xEC, 0x14,
	}
	intCodec, err := goavro.NewCodec(`"int"`)
	require.NoError(t, err)
	stringCodec, err := goavro.NewCodec(`"string"`)
	require.NoError(t, err)

	_ = intCodec
	_ = stringCodec

	y := make([]byte, 0)
	z, err := stringCodec.BinaryFromNative(y, "tttt")
	fmt.Printf("%X / %v\n", z, err)
	x, b, err := intCodec.NativeFromBinary(b)
	fmt.Printf("%v / %X / %v\n", x, b, err)
}

func TestClient_Append2(t *testing.T) {
	addr, clean := internal.RunServer(t, func(conn net.Conn) error {
		req := internal.Buffer{}
		for {
			_, err := req.ReadFrom(conn)
			if err == io.EOF {
				return nil
			}
			if err != nil {

				return err
			}
			// Handshake request
			if bytes.Equal(req.Bytes(), []byte{
				// The frame serial: 1
				0x00, 0x00, 0x00, 0x01,
				// The number of frames: 1
				0x00, 0x00, 0x00, 0x01,
				// The frame length: 36
				0x00, 0x00, 0x00, 0x24,
				// MD5 hash of the client message protocol
				0x49, 0x87, 0x43, 0x7B, 0xF5, 0x09, 0xDF, 0xDE, 0x62, 0x36, 0x72, 0x99, 0xEF, 0x40, 0xC8, 0x2F,
				// The client message protocol, don't pass it by default: null
				0x00,
				// MD5 hash of the server message protocol that already known by client for the server
				0x49, 0x87, 0x43, 0x7B, 0xF5, 0x09, 0xDF, 0xDE, 0x62, 0x36, 0x72, 0x99, 0xEF, 0x40, 0xC8, 0x2F,
				// Meta
				0x00,
				// Empty message
				0x00, 0x00,
			}) {
				req.Reset()
				_, err := conn.Write([]byte{
					// The frame serial: 1
					0x00, 0x00, 0x00, 0x01,
					// The number of frames: 1
					0x00, 0x00, 0x00, 0x01,
					// The frame length: 25
					0x00, 0x00, 0x00, 0x19,
					// Match field: NONE
					0x02,
					// The server message protocol: type(string):length(4):value(tttt)
					0x02, 0x08, 0x74, 0x74, 0x74, 0x74,
					// MD5 hash of the server message protocol: type(MD5):value(...)
					0x02, 0x86, 0xAA, 0xDA, 0xE2, 0xC4, 0x54, 0x74, 0xC0, 0xFE, 0x93, 0xFF, 0xD0, 0xF2, 0x35, 0x0A, 0x65,
					// Meta
					0x00,
				})
				if err != nil {
					return err
				}
			}
			if bytes.Equal(req.Bytes(), []byte{
				// The frame serial: 2
				0x00, 0x00, 0x00, 0x02,
				// The number of frames: 1
				0x00, 0x00, 0x00, 0x01,
				// The frame length: 14
				0x00, 0x00, 0x00, 0x0E,
				// Meta
				0x00,
				// Method: length(6):value(append)
				0x0C, 0x61, 0x70, 0x70, 0x65, 0x6E, 0x64,
				// Event header
				0x00,
				// Event body: length(4):value(tttt)
				0x08, 0x74, 0x74, 0x74, 0x74,
			}) {
				req.Reset()
				_, err := conn.Write([]byte{
					// The frame serial: 2
					0x00, 0x00, 0x00, 0x02,
					// The number of frames: 3
					0x00, 0x00, 0x00, 0x03,
					// The frame length: 0
					0x00, 0x00, 0x00, 0x00,
					// The frame length: 1
					0x00, 0x00, 0x00, 0x01,
					// Meta
					0x00,
					// The frame length: 2
					0x00, 0x00, 0x00, 0x02,
					// Response flag
					0x00,
					// Response status
					0x00,
				})
				if err != nil {
					return err
				}
			}
		}
	})

	client, err := avroipc.NewClient(addr, 1*time.Second, 3*time.Second, 0, 0)
	require.NoError(t, err)

	event := &avroipc.Event{
		Body: []byte("tttt"),
	}
	status, err := client.Append(event)
	require.NoError(t, err)
	require.Equal(t, "OK", status)

	require.NoError(t, client.Close())
	require.NoError(t, clean())
}
