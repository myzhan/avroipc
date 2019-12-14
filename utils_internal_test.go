package avroipc

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_recvBytes(t *testing.T) {
	t.Run("zero value", func(t *testing.T) {
		buf := bytes.NewBuffer([]byte("test"))
		actual := recvBytes(buf, 2)
		expected := []byte{0x74, 0x65}

		require.Equal(t, expected, actual)
	})

	t.Run("normal value", func(t *testing.T) {
		buf := bytes.NewBuffer([]byte("test"))
		actual := recvBytes(buf, 4)
		expected := []byte{0x74, 0x65, 0x73, 0x74}

		require.Equal(t, expected, actual)
	})
}

func Test_encodeInt(t *testing.T) {
	t.Run("zero value", func(t *testing.T) {
		actual := encodeInt(0)
		expected := []byte{0x0}

		require.Equal(t, expected, actual)
	})

	t.Run("normal value", func(t *testing.T) {
		actual := encodeInt(7)
		expected := []byte{0xe}

		require.Equal(t, expected, actual)
	})
}

func Test_messageHeader(t *testing.T) {
	actual := messageHeader()
	expected := []byte{
		// Meta header
		0x0,
		// Method name length
		0xc,
		// Method name: append
		0x61, 0x70, 0x70, 0x65, 0x6e, 0x64}

	require.Equal(t, expected, actual)
}

func Test_getMD5(t *testing.T) {
	t.Run("no values", func(t *testing.T) {
		actual := getMD5()
		expected := []byte{0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x0, 0xb2, 0x4, 0xe9, 0x80, 0x9, 0x98, 0xec, 0xf8, 0x42, 0x7e}

		require.Equal(t, expected, actual)
	})

	t.Run("single value", func(t *testing.T) {
		actual := getMD5("test string")
		expected := []byte{0x6f, 0x8d, 0xb5, 0x99, 0xde, 0x98, 0x6f, 0xab, 0x7a, 0x21, 0x62, 0x5b, 0x79, 0x16, 0x58, 0x9c}

		require.Equal(t, expected, actual)
	})

	t.Run("multiple values", func(t *testing.T) {
		actual := getMD5("string 1", "string 2")
		expected := []byte{0x60, 0x29, 0x64, 0x59, 0xb, 0x60, 0x61, 0x92, 0x57, 0x8c, 0xf3, 0x2b, 0xdb, 0x3a, 0xa8, 0x58}

		require.Equal(t, expected, actual)
	})
}
