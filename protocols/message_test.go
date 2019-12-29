package protocols_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/myzhan/avroipc/protocols"
)

func makeDatum(body string) interface{} {
	return map[string]interface{}{
		"headers": map[string]interface{}{},
		"body":    body,
	}
}

func makeArrayDatum(bodies ...string) interface{} {
	result := make([]interface{}, len(bodies))
	for i, body := range bodies {
		result[i] = makeDatum(body)
	}
	return result
}

func TestAvroSourceProtocol_PrepareMessage(t *testing.T) {
	p, err := protocols.NewAvroSourceProtocol()
	require.NoError(t, err)

	t.Run("bad method", func(t *testing.T) {
		_, err := p.PrepareMessage("bad method", "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown method name: bad method")
	})

	t.Run("append empty", func(t *testing.T) {
		actual, err := p.PrepareMessage("append", makeDatum(""))
		require.NoError(t, err)
		require.Equal(t, []byte{0x0, 0x0}, actual)
	})
	t.Run("append not empty", func(t *testing.T) {
		actual, err := p.PrepareMessage("append", makeDatum("not empty"))
		require.NoError(t, err)
		require.Equal(t, []byte{0x0, 0x12, 0x6e, 0x6f, 0x74, 0x20, 0x65, 0x6d, 0x70, 0x74, 0x79}, actual)
	})
	t.Run("append bad datum", func(t *testing.T) {
		_, err := p.PrepareMessage("append", "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot encode binary record")
	})

	t.Run("appendBatch empty", func(t *testing.T) {
		actual, err := p.PrepareMessage("appendBatch", makeArrayDatum(""))
		require.NoError(t, err)
		require.Equal(t, []byte{0x2, 0x0, 0x0, 0x0}, actual)
	})
	t.Run("appendBatch not empty", func(t *testing.T) {
		actual, err := p.PrepareMessage("appendBatch", makeArrayDatum("a", "b"))
		require.NoError(t, err)
		require.Equal(t, []byte{0x4, 0x0, 0x2, 0x61, 0x0, 0x2, 0x62, 0x0}, actual)
	})
	t.Run("appendBatch bad datum", func(t *testing.T) {
		_, err := p.PrepareMessage("appendBatch", "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot encode binary array")
	})
}

func TestAvroSourceProtocol_ParseMessage(t *testing.T) {
	p, err := protocols.NewAvroSourceProtocol()
	require.NoError(t, err)

	t.Run("bad method", func(t *testing.T) {
		_, _, err := p.ParseMessage("bad method", []byte{0x1, 0x7})
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown method name: bad method")
	})
	for _, method := range []string{"append", "appendBatch"} {
		t.Run(method+" ok", func(t *testing.T) {
			actual, bytes, err := p.ParseMessage(method, []byte{0x0})
			require.NoError(t, err)
			require.Equal(t, []byte{}, bytes)
			require.Equal(t, "OK", actual)
		})
		t.Run(method+" failed", func(t *testing.T) {
			actual, bytes, err := p.ParseMessage(method, []byte{0x2, 0x7})
			require.NoError(t, err)
			require.Equal(t, []byte{0x7}, bytes)
			require.Equal(t, "FAILED", actual)
		})
		t.Run(method+" bad response", func(t *testing.T) {
			_, _, err := p.ParseMessage(method, []byte{0x1, 0x7})
			require.Error(t, err)
			require.Contains(t, err.Error(), "cannot decode binary enum")
		})
	}
}

func TestAvroSourceProtocol_ParseError(t *testing.T) {
	p, err := protocols.NewAvroSourceProtocol()
	require.NoError(t, err)

	t.Run("bad method", func(t *testing.T) {
		_, err := p.ParseError("bad method", []byte{0x1, 0x7})
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown method name: bad method")
	})
	for _, method := range []string{"append", "appendBatch"} {
		t.Run(method+" ok", func(t *testing.T) {
			bytes, err := p.ParseError(method, []byte{0x0, 0x12, 0x6e, 0x6f, 0x74, 0x20, 0x65, 0x6d, 0x70, 0x74, 0x79})
			require.Error(t, err)
			require.Contains(t, err.Error(), "not empty")
			require.Equal(t, []byte{}, bytes)
		})
		t.Run(method+" short buffer", func(t *testing.T) {
			bytes, err := p.ParseError(method, []byte{0x0})
			require.Error(t, err)
			require.Contains(t, err.Error(), "cannot decode binary bytes: short buffer")
			require.Equal(t, []byte{0x0}, bytes)
		})
		t.Run(method+" bad response", func(t *testing.T) {
			bytes, err := p.ParseError(method, []byte{0x1, 0x7})
			require.Error(t, err)
			require.Contains(t, err.Error(), "cannot decode binary union")
			require.Equal(t, []byte{0x1, 0x7}, bytes)
		})
	}
}
