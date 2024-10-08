package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

type RequestType uint32

const (
	REQUEST_METADATA RequestType = iota
	REQUEST_READ     RequestType = iota
	REQUEST_WRITE    RequestType = iota
)

type Request interface {
	GetType() RequestType
}

type MetaDataRequest struct {
}

func (*MetaDataRequest) GetType() RequestType {
	return REQUEST_METADATA
}

type ReadRequest struct {
	Topic     string
	Partition uint32
}

func (*ReadRequest) GetType() RequestType {
	return REQUEST_READ
}

type WriteRequest struct {
	Topic     string
	Partition uint32
	Message   []byte
}

func (*WriteRequest) GetType() RequestType {
	return REQUEST_WRITE
}

var (
	ErrUnknownRequestType = errors.New("unknown request type")
)

var NetworkOder = binary.BigEndian

func DecodeRequest(r io.Reader) (Request, error) {
	var requestType RequestType
	err := binary.Read(r, NetworkOder, &requestType)
	if err != nil {
		return nil, err
	}

	switch requestType {
	case REQUEST_METADATA:
		return decodeMetadataRequest(r)

	case REQUEST_READ:
		return decodeReadRequest(r)

	case REQUEST_WRITE:
		return decodeWriteRequest(r)

	default:
		return nil, ErrUnknownRequestType
	}
}

func decodeMetadataRequest(io.Reader) (*MetaDataRequest, error) {
	return &MetaDataRequest{}, nil
}

func decodeReadRequest(r io.Reader) (*ReadRequest, error) {
	// Decoding topic
	var n uint32
	err := binary.Read(r, NetworkOder, &n)
	if err != nil {
		return nil, err
	}

	topic := make([]byte, n)
	err = binary.Read(r, NetworkOder, topic)
	if err != nil {
		return nil, err
	}

	// Decoding partition
	var partition uint32
	err = binary.Read(r, NetworkOder, &partition)
	if err != nil {
		return nil, err
	}

	return &ReadRequest{
		Topic:     string(topic),
		Partition: partition,
	}, nil
}

func decodeWriteRequest(r io.Reader) (*WriteRequest, error) {
	// Decoding topic
	var n uint32
	err := binary.Read(r, NetworkOder, &n)
	if err != nil {
		return nil, err
	}

	topic := make([]byte, n)
	err = binary.Read(r, NetworkOder, topic)
	if err != nil {
		return nil, err
	}

	// Decoding partition
	var partition uint32
	err = binary.Read(r, NetworkOder, &partition)
	if err != nil {
		return nil, err
	}

	//Decoding message
	var messageLen uint32
	err = binary.Read(r, NetworkOder, &messageLen)
	if err != nil {
		return nil, err
	}

	message := make([]byte, messageLen)
	err = binary.Read(r, NetworkOder, message)
	if err != nil {
		return nil, err
	}

	return &WriteRequest{
		Topic:     string(topic),
		Partition: partition,
		Message:   message,
	}, nil
}

func EncodeRequest(w io.Writer, req Request) error {
	requestType := req.GetType()
	err := binary.Write(w, NetworkOder, &requestType)
	if err != nil {
		return err
	}

	switch requestType {
	case REQUEST_METADATA:
		return encodeMetadataRequest(w, req.(*MetaDataRequest))

	case REQUEST_READ:
		return encodeReadRequest(w, req.(*ReadRequest))

	case REQUEST_WRITE:
		return encodeWriteRequest(w, req.(*WriteRequest))

	default:
		return ErrUnknownRequestType
	}
}

func encodeMetadataRequest(io.Writer, *MetaDataRequest) error {
	return nil
}

func encodeReadRequest(w io.Writer, req *ReadRequest) error {
	// Encoding topic
	topic := []byte(req.Topic)
	err := binary.Write(w, NetworkOder, uint32(len(topic)))
	if err != nil {
		return err
	}

	err = binary.Write(w, NetworkOder, topic)
	if err != nil {
		return err
	}

	// Encoding partition
	err = binary.Write(w, NetworkOder, req.Partition)
	if err != nil {
		return err
	}

	return nil
}

func encodeWriteRequest(w io.Writer, req *WriteRequest) error {
	// Encoding topic
	topic := []byte(req.Topic)
	err := binary.Write(w, NetworkOder, uint32(len(topic)))
	if err != nil {
		return err
	}

	err = binary.Write(w, NetworkOder, topic)
	if err != nil {
		return err
	}

	// Encoding partition
	err = binary.Write(w, NetworkOder, req.Partition)
	if err != nil {
		return err
	}

	// Encoding Message
	err = binary.Write(w, NetworkOder, uint32(len(req.Message)))
	if err != nil {
		return err
	}

	err = binary.Write(w, NetworkOder, req.Message)
	if err != nil {
		return err
	}

	return nil
}
