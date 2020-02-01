package flume

import (
	"github.com/myzhan/avroipc"
)

// An avro client implementation
type Client interface {
	Close() error
	Append(event *Event) (string, error)
	AppendBatch(events []*Event) (string, error)
}

type client struct {
	client avroipc.Client
}

// NewClient creates an avro client with default option values and
// connects to the specified remote Flume endpoint immediately.
//
// Very useful for the testing purposes and to build simple examples.
func NewClient(addr string) (Client, error) {
	return NewClientWithConfig(addr, avroipc.NewConfig())
}

// NewClient creates an avro client with considering values of options from
// the passed configuration object and connects to the specified remote Flume
// endpoint immediately.
//
// This constructor supposed to be used in production environments.
func NewClientWithConfig(addr string, config *avroipc.Config) (Client, error) {
	c, err := avroipc.NewClientWithConfig(addr, config)
	if err != nil {
		return nil, err
	}

	return &client{c}, nil
}

// Append sends event to flume
func (c *client) Append(event *Event) (string, error) {
	datum := event.toMap()

	return c.client.SendMessage("append", datum)
}

// Append sends events to flume
func (c *client) AppendBatch(events []*Event) (string, error) {
	datum := make([]map[string]interface{}, 0)
	for _, event := range events {
		datum = append(datum, event.toMap())
	}

	return c.client.SendMessage("appendBatch", datum)
}

func (c *client) Close() error {
	return c.client.Close()
}
