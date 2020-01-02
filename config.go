package avroipc

import "time"

// Config provides a configuration for the client. Use the NewConfig method
// to create an instance of the Config and set all necessary parameters of
// the configuration.
type Config struct {
	// Connection timeout for the built-in socket transport that limit time of
	// connection to a Flume server.
	//
	// It is sane to always set this timeout before creating a new client
	// instance because it will protect against hanging clients.
	//
	// Defaults to zero which means disabled connection timeout.
	Timeout time.Duration

	// Used to set read and write deadline of the built-in transports
	// (actually, affects only the socket transport). It sets both deadlines
	// together at the same time and there is no way to set them separately.
	// These deadlines are used to limit execution time of reading and writing
	// operations.
	//
	// This timeout is supposed to be always set to any appropriate for
	// a particular situation value except maybe test and example
	// configurations.
	//
	// Defaults to zero which means disabled read/write timeouts.
	SendTimeout time.Duration

	// A buffer size of the built-in buffered transport.
	//
	// Defaults to zero which means that the buffered transport won't be used.
	BufferSize int
	// A compression level of the built-in zlib transport.
	//
	// Defaults to zero which means that the compression will be disabled.
	CompressionLevel int
}

// NewConfig returns a pointer to a new Config instance that is used to
// configure the client at a creation time. Invoking methods of the config
// instance may be chained with each other to specify all necessary config
// options in a single command. A NewConfig call may be also chained with
// other methods to inline config creations.
//
//     config := NewConfig()
//     config.WithTimeout(3*time.Second)
//     client, err := NewClientWithConfig(config)
// or just
//     client, err := NewClientWithConfig(NewConfig().WithTimeout(3*time.Second))
func NewConfig() *Config {
	return &Config{}
}

// Sets the connection timeout.
func (c *Config) WithTimeout(t time.Duration) *Config {
	c.Timeout = t
	return c
}

// Sets the read/write timeouts together.
func (c *Config) WithSendTimeout(t time.Duration) *Config {
	c.SendTimeout = t
	return c
}

// Sets size of the internal buffer of the buffered transport.
func (c *Config) WithBufferSize(s int) *Config {
	c.BufferSize = s
	return c
}

// Sets the compression level of the zlib transport.
func (c *Config) WithCompressionLevel(l int) *Config {
	c.CompressionLevel = l
	return c
}
