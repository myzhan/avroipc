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
