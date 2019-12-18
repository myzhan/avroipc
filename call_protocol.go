package avroipc

import (
	"bytes"
	"fmt"
	"github.com/linkedin/goavro"
)

type CallProtocol interface {
	PrepareRequest(method string, datum interface{}) ([]byte, error)
	ParseResponse(method string, responseBytes []byte) (interface{}, []byte, error)
}

// The Avro Call format implementation for the Avro RPC protocol.
//
// It is used for preparing an Avro RPC request and parsing an Avro RPC response.
//
// See http://avro.apache.org/docs/1.8.2/spec.html#Call+Format for details.
type сallProtocol struct {
	proto MessageProtocol

	metaCodec    *goavro.Codec
	stringCodec  *goavro.Codec
	booleanCodec *goavro.Codec
}

func NewCallProtocol(proto MessageProtocol) (CallProtocol, error) {
	p := &сallProtocol{
		proto: proto,
	}

	err := p.init()
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *сallProtocol) init() (err error) {
	p.metaCodec, err = goavro.NewCodec(`{"type": "map", "values": "bytes"}`)
	if err != nil {
		return
	}
	p.stringCodec, err = goavro.NewCodec(`"string"`)
	if err != nil {
		return
	}
	p.booleanCodec, err = goavro.NewCodec(`"boolean"`)
	if err != nil {
		return
	}

	return
}

func (p *сallProtocol) PrepareRequest(method string, datum interface{}) ([]byte, error) {
	meta := make(map[string][]byte)
	metaBytes, err := p.metaCodec.BinaryFromNative(nil, meta)
	if err != nil {
		return nil, err
	}

	methodBytes, err := p.stringCodec.BinaryFromNative(nil, method)
	if err != nil {
		return nil, err
	}

	paramsBytes, err := p.proto.PrepareMessage(method, datum)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(metaBytes)
	buf.Write(methodBytes)
	buf.Write(paramsBytes)

	return buf.Bytes(), nil
}

func (p *сallProtocol) ParseResponse(method string, responseBytes []byte) (interface{}, []byte, error) {
	meta, responseBytes, err := p.metaCodec.NativeFromBinary(responseBytes)
	if err != nil {
		return nil, responseBytes, err
	}
	_ = meta

	flag, responseBytes, err := p.booleanCodec.NativeFromBinary(responseBytes)
	if err != nil {
		return nil, responseBytes, err
	}
	flagBool, ok := flag.(bool)
	if !ok {
		return nil, responseBytes, fmt.Errorf("cannot convert error flag to boolean: %v", flag)
	}

	if flagBool {
		responseBytes, err = p.proto.ParseError(method, responseBytes)
		return nil, responseBytes, err
	}

	return p.proto.ParseMessage(method, responseBytes)
}
