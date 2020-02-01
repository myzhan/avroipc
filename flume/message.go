package flume

import (
	"fmt"

	"github.com/myzhan/avroipc/protocols"

	"github.com/linkedin/goavro"
)

type message struct {
	request  *goavro.Codec
	response *goavro.Codec
	errors   *goavro.Codec
}

// The Avro message protocol implementation for the Avro RPC protocol for using with the Avro Flume Source.
//
// It has used for preparing an outgoing message from input data and parsing a response message.
//
// The Avro Flume Source haven't documented well now.
type AvroSourceProtocol struct {
	messages map[string]message
}

func NewAvroSource() (protocols.MessageProtocol, error) {
	p := &AvroSourceProtocol{
		messages: make(map[string]message),
	}

	err := p.init()
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *AvroSourceProtocol) init() (err error) {
	eventCodec, err := goavro.NewCodec(eventSchema)
	if err != nil {
		return
	}
	eventsCodec, err := goavro.NewCodec(eventsSchema)
	if err != nil {
		return
	}
	errorsCodec, err := goavro.NewCodec(errorsSchema)
	if err != nil {
		return
	}
	statusCodec, err := goavro.NewCodec(statusSchema)
	if err != nil {
		return
	}
	p.messages["append"] = message{eventCodec, statusCodec, errorsCodec}
	p.messages["appendBatch"] = message{eventsCodec, statusCodec, errorsCodec}

	return
}

func (p *AvroSourceProtocol) PrepareMessage(method string, datum interface{}) ([]byte, error) {
	message, ok := p.messages[method]
	if !ok {
		return nil, fmt.Errorf("unknown method name: %s", method)
	}

	return message.request.BinaryFromNative(nil, datum)
}

func (p *AvroSourceProtocol) ParseMessage(method string, responseBytes []byte) (interface{}, []byte, error) {
	message, ok := p.messages[method]
	if !ok {
		return nil, responseBytes, fmt.Errorf("unknown method name: %s", method)
	}

	return message.response.NativeFromBinary(responseBytes)
}

func (p *AvroSourceProtocol) ParseError(method string, responseBytes []byte) ([]byte, error) {
	message, ok := p.messages[method]
	if !ok {
		return responseBytes, fmt.Errorf("unknown method name: %s", method)
	}

	response, responseBytes, err := message.errors.NativeFromBinary(responseBytes)
	if err != nil {
		return responseBytes, err
	}

	responseMap, ok := response.(map[string]interface{})
	if !ok {
		return responseBytes, fmt.Errorf("cannot convert error union to map: %v", response)
	}

	responseInt, ok := responseMap["string"]
	if !ok {
		return responseBytes, fmt.Errorf("string error not found in map: %v", responseMap)
	}

	responseStr, ok := responseInt.(string)
	if !ok {
		return responseBytes, fmt.Errorf("cannot convert string error to string: %v", responseInt)
	}

	return responseBytes, fmt.Errorf(responseStr)
}
