package avroipc

import (
	"bytes"
	"crypto/md5"
	"github.com/linkedin/goavro"
	"io"
	"log"
)

func recvBytes(conn io.Reader, length int) []byte {
	buf := make([]byte, length)
	for length > 0 {
		n, err := conn.Read(buf)
		if err != nil {
			log.Fatal(err)
		}
		length = length - n
	}
	return buf
}

// avro-specify int
func encodeInt(n int) []byte {
	codec, err := goavro.NewCodec(`"int"`)
	if err != nil {
		log.Fatalf("%v", err)
	}
	bin, err := codec.BinaryFromNative(nil, n)
	if err != nil {
		log.Fatalf("%v", err)
	}
	return bin
}

func messageHeader() []byte {
	buf := new(bytes.Buffer)
	// meta header isn't supported so far, write an empty meta header, which is 0
	buf.WriteByte(0)
	// write length of "append"
	buf.Write(encodeInt(len("append")))
	buf.Write([]byte("append"))

	return buf.Bytes()
}

func getMD5(slice ...string) []byte {
	h := md5.New()
	for _, v := range slice {
		io.WriteString(h, v)
	}
	return h.Sum(nil)
}
