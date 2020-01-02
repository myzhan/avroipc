package avroipc_test

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/myzhan/avroipc/internal"

	"github.com/stretchr/testify/require"

	"github.com/myzhan/avroipc"
)

type pair struct {
	req  []byte
	resp []byte
}

func TestClient(t *testing.T) {
	data := map[string]struct {
		pairs []pair

		level  int
		buffer int
	}{
		"plain data": {
			level:  0,
			buffer: 0,
			pairs: []pair{{
				// Handshake request
				req: []byte{
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
				},
				resp: []byte{
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
				},
			}, {
				// Regular append call
				req: []byte{
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
				},
				resp: []byte{
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
				},
			}},
		},
		"compressed data": {
			level:  6,
			buffer: 1024,
			pairs: []pair{{
				// Handshake request
				req: []byte{
					// Compressed data
					0x78, 0x9C,
					0x62, 0x60, 0x60, 0x60, 0x84, 0x62, 0x15, 0xCF, 0x76, 0xE7, 0xEA, 0xAF, 0x9C, 0xF7, 0xEF, 0x25, 0x99, 0x15, 0xCD, 0x7C, 0xEF, 0x70, 0x42, 0x9F, 0x01, 0x43, 0x80, 0x81, 0x01, 0x00,
					0x00, 0x00, 0xFF, 0xFF,
				},
				resp: []byte{
					// Compressed data
					0x78, 0x9C,
					0x62, 0x60, 0x60, 0x60, 0x84, 0x62, 0x49, 0x26, 0x26, 0x8E, 0x92, 0x92, 0x92, 0x12, 0xA6, 0xB6, 0x55, 0xB7, 0x1E, 0x1D, 0x09, 0x29, 0x39, 0xF0, 0x6F, 0xF2, 0xFF, 0x0B, 0x9F, 0x4C, 0xB9, 0x52, 0x19, 0x00, 0x00,
					0x00, 0x00, 0xFF, 0xFF,
				},
			}, {
				// Regular append call
				req: []byte{
					// Compressed data
					0x62, 0x60, 0x60, 0x60, 0x82, 0xAA, 0xE7, 0x63, 0xE0, 0x49, 0x2C, 0x28, 0x48, 0xCD, 0x4B, 0x61, 0xE0, 0x28, 0x29, 0x29, 0x29, 0x01, 0x00,
					0x00, 0x00, 0xFF, 0xFF,
				},
				resp: []byte{
					// Compressed data
					0x62, 0x60, 0x60, 0x60, 0x62, 0x60, 0x60, 0x60, 0x66, 0x80, 0x00, 0x90, 0x62, 0x90, 0x00, 0x00,
					0x00, 0x00, 0xFF, 0xFF,
				},
			}},
		},
	}
	for n, d := range data {
		t.Run(n, func(t *testing.T) {
			addr, clean := internal.RunServer(t, func(conn net.Conn) error {
				req := internal.Buffer{}
				for {
					err := req.ReadFrom(conn)
					if err == io.EOF {
						return nil
					}
					if err != nil {
						return err
					}

					for _, p := range d.pairs {
						if bytes.Equal(req.Bytes(), p.req) {
							req.Reset()
							_, err := conn.Write(p.resp)
							if err != nil {
								return err
							}
						}
					}
				}
			})

			config := avroipc.NewConfig()
			config.WithTimeout(time.Second)
			config.WithSendTimeout(3 * time.Second)
			config.WithBufferSize(d.buffer)
			config.WithCompressionLevel(d.level)
			client, err := avroipc.NewClientWithConfig(addr, config)
			require.NoError(t, err)

			event := &avroipc.Event{
				Body: []byte("tttt"),
			}
			status, err := client.Append(event)
			require.NoError(t, err)
			require.Equal(t, "OK", status)

			require.NoError(t, client.Close())
			require.NoError(t, clean())
		})
	}

	t.Run("bad address", func(t *testing.T) {
		_, err := avroipc.NewClient("1:2:3")
		require.Error(t, err)
		require.Contains(t, err.Error(), "too many colons in address")
	})

	t.Run("bad compression level", func(t *testing.T) {
		addr, clean := internal.RunServer(t, func(conn net.Conn) error {
			return nil
		})
		_, err := avroipc.NewClientWithConfig(addr, avroipc.NewConfig().WithCompressionLevel(10))
		require.Error(t, err)
		require.Contains(t, err.Error(), "zlib: invalid compression level: 10")

		require.NoError(t, clean())
	})
}
