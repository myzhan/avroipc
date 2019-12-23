package avroipc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const maxFrameSize = 10 * 1024

type FramingLayer interface {
	Read() ([]byte, error)

	Write(p []byte) error

	Close() error

	SetDeadline(t time.Time) error
}

// Framing is a part on the Avro RPC protocol.
// Framing just is a layer between messages and the transport, it isn't transport.
type framingLayer struct {
	rb bytes.Buffer

	trans Transport

	serial uint32
}

func NewFramingLayer(trans Transport) FramingLayer {
	return &framingLayer{
		trans: trans,
	}
}

func (f *framingLayer) Read() ([]byte, error) {
	err := f.readFrames()
	if err != nil {
		return nil, err
	}

	b := f.rb.Bytes()
	f.rb.Reset()
	return b, err
}

func (f *framingLayer) readFrames() error {
	serial, err := f.readUint32()
	if err != nil {
		return err
	}
	if f.serial != serial {
		return fmt.Errorf("bad serial: %d != %d", f.serial, serial)
	}

	frames, err := f.readUint32()
	if err != nil {
		return err
	}

	for i := uint32(0); i < frames; i++ {
		size, err := f.readUint32()
		if err != nil {
			return err
		}

		frame := make([]byte, int(size))
		_, err = io.ReadFull(f.trans, frame)
		if err != nil {
			return err
		}
		f.rb.Write(frame)
	}

	return nil
}

func (f *framingLayer) readUint32() (uint32, error) {
	buf := make([]byte, 4)
	_, err := io.ReadFull(f.trans, buf)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(buf), nil
}

func (f *framingLayer) Write(p []byte) error {
	f.serial++

	if len(p) > 0 {
		err := f.writeFrames(p)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *framingLayer) writeFrames(p []byte) (err error) {
	bufLen := len(p)
	frames := (bufLen-1)/maxFrameSize + 1

	err = binary.Write(f.trans, binary.BigEndian, f.serial)
	if err != nil {
		return
	}
	err = binary.Write(f.trans, binary.BigEndian, uint32(frames))
	if err != nil {
		return
	}

	for len(p) >= maxFrameSize {
		binary.Write(f.trans, binary.BigEndian, uint32(maxFrameSize))
		_, err = f.trans.Write(p[:maxFrameSize])
		if err != nil {
			return
		}
		p = p[maxFrameSize:]
	}

	err = binary.Write(f.trans, binary.BigEndian, uint32(len(p)))
	if err != nil {
		return
	}

	_, err = f.trans.Write(p)
	if err != nil {
		return
	}

	return
}

func (f *framingLayer) Close() error {
	return f.trans.Close()
}

func (f *framingLayer) SetDeadline(d time.Time) error {
	return f.trans.SetDeadline(d)
}
