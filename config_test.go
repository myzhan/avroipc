package avroipc_test

import (
	"testing"
	"time"

	"github.com/myzhan/avroipc"

	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	c := avroipc.NewConfig()
	c.WithTimeout(1)
	c.WithSendTimeout(2)
	c.WithBufferSize(3)
	c.WithCompressionLevel(4)

	require.Equal(t, time.Duration(1), c.Timeout)
	require.Equal(t, time.Duration(2), c.SendTimeout)
	require.Equal(t, 3, c.BufferSize)
	require.Equal(t, 4, c.CompressionLevel)
}
