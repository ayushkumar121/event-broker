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
	KeepAlive() bool
}

type MetaDataRequest struct {
}

func (*MetaDataRequest) GetType() RequestType {
	return REQUEST_METADATA
}

func (*MetaDataRequest) KeepAlive() bool {
	return false
}

type ReadRequest struct {
	Topic      string
	Partition  uint32
	LastOffset Offset
}

func (*ReadRequest) GetType() RequestType {
	return REQUEST_READ
}

func (*ReadRequest) KeepAlive() bool {
	return true
}

type WriteRequest struct {
	Topic     string
	Partition uint32
	Message   []byte
}

func (*WriteRequest) GetType() RequestType {
	return REQUEST_WRITE
}

func (*WriteRequest) KeepAlive() bool {
	return false
}

var NetworkOrder = binary.BigEndian

func DecodeRequest(r io.Reader) (Request, error) {
	var requestType RequestType
	err := binary.Read(r, NetworkOrder, &requestType)
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
		return nil, errors.New("unknown request type")
	}
}

func decodeMetadataRequest(io.Reader) (*MetaDataRequest, error) {
	return &MetaDataRequest{}, nil
}

func decodeReadRequest(r io.Reader) (*ReadRequest, error) {
	// Decoding topic
	var n uint32
	err := binary.Read(r, NetworkOrder, &n)
	if err != nil {
		return nil, err
	}

	topic := make([]byte, n)
	err = binary.Read(r, NetworkOrder, topic)
	if err != nil {
		return nil, err
	}

	// Decoding partition
	var partition uint32
	err = binary.Read(r, NetworkOrder, &partition)
	if err != nil {
		return nil, err
	}

	// Decoding last offset
	var lastOffset Offset
	err = binary.Read(r, NetworkOrder, &lastOffset)
	if err != nil {
		return nil, err
	}

	return &ReadRequest{
		Topic:      string(topic),
		Partition:  partition,
		LastOffset: lastOffset,
	}, nil
}

func decodeWriteRequest(r io.Reader) (*WriteRequest, error) {
	// Decoding topic
	var n uint32
	err := binary.Read(r, NetworkOrder, &n)
	if err != nil {
		return nil, err
	}

	topic := make([]byte, n)
	err = binary.Read(r, NetworkOrder, topic)
	if err != nil {
		return nil, err
	}

	// Decoding partition
	var partition uint32
	err = binary.Read(r, NetworkOrder, &partition)
	if err != nil {
		return nil, err
	}

	//Decoding message
	var messageLen uint32
	err = binary.Read(r, NetworkOrder, &messageLen)
	if err != nil {
		return nil, err
	}

	message := make([]byte, messageLen)
	err = binary.Read(r, NetworkOrder, message)
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
	err := binary.Write(w, NetworkOrder, &requestType)
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
		return errors.New("unknown request type")
	}
}

func encodeMetadataRequest(io.Writer, *MetaDataRequest) error {
	return nil
}

func encodeReadRequest(w io.Writer, req *ReadRequest) error {
	// Encoding topic
	topic := []byte(req.Topic)
	err := binary.Write(w, NetworkOrder, uint32(len(topic)))
	if err != nil {
		return err
	}

	err = binary.Write(w, NetworkOrder, topic)
	if err != nil {
		return err
	}

	// Encoding partition
	err = binary.Write(w, NetworkOrder, req.Partition)
	if err != nil {
		return err
	}

	// Encoding last offset
	err = binary.Write(w, NetworkOrder, req.LastOffset)
	if err != nil {
		return err
	}

	return nil
}

func encodeWriteRequest(w io.Writer, req *WriteRequest) error {
	// Encoding topic
	topic := []byte(req.Topic)
	err := binary.Write(w, NetworkOrder, uint32(len(topic)))
	if err != nil {
		return err
	}

	err = binary.Write(w, NetworkOrder, topic)
	if err != nil {
		return err
	}

	// Encoding partition
	err = binary.Write(w, NetworkOrder, req.Partition)
	if err != nil {
		return err
	}

	// Encoding Message
	err = binary.Write(w, NetworkOrder, uint32(len(req.Message)))
	if err != nil {
		return err
	}

	err = binary.Write(w, NetworkOrder, req.Message)
	if err != nil {
		return err
	}

	return nil
}
