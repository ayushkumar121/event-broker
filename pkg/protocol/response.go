package protocol

import (
	"encoding/binary"
	"io"
)

type ResponseType uint32

const (
	RESPONSE_METADATA ResponseType = iota
	RESPONSE_READ     ResponseType = iota
	RESPONSE_WRITE    ResponseType = iota
	RESPONSE_ERROR    ResponseType = iota
)

type Response interface {
	GetType() ResponseType
}

type MetaDataResponse struct {
}

func (*MetaDataResponse) GetType() ResponseType {
	return RESPONSE_METADATA
}

type ReadResponse struct {
	Offset  uint64
	Message []byte
}

func (*ReadResponse) GetType() ResponseType {
	return RESPONSE_READ
}

type WriteResponse struct {
	Offset int64
}

func (*WriteResponse) GetType() ResponseType {
	return RESPONSE_WRITE
}

type ErrorResponse struct {
	Message string
}

func (*ErrorResponse) GetType() ResponseType {
	return RESPONSE_ERROR
}

func DecodeResponse(r io.Reader) (Response, error) {
	var responseType ResponseType
	err := binary.Read(r, NetworkOrder, &responseType)
	if err != nil {
		return nil, err
	}

	switch responseType {
	case RESPONSE_METADATA:
		return decodeMetadataResponse(r)

	case RESPONSE_READ:
		return decodeReadResponse(r)

	case RESPONSE_WRITE:
		return decodeWriteResponse(r)

	case RESPONSE_ERROR:
		return decodeErrorResponse(r)

	default:
		return nil, ErrUnknownRequestType
	}
}

func decodeMetadataResponse(io.Reader) (*MetaDataResponse, error) {
	return &MetaDataResponse{}, nil
}

func decodeReadResponse(r io.Reader) (*ReadResponse, error) {
	// Decoding offset
	var offset uint64
	err := binary.Read(r, NetworkOrder, &offset)
	if err != nil {
		return nil, err
	}

	// Decoding message
	var n uint32
	err = binary.Read(r, NetworkOrder, &n)
	if err != nil {
		return nil, err
	}

	message := make([]byte, n)
	err = binary.Read(r, NetworkOrder, message)
	if err != nil {
		return nil, err
	}

	return &ReadResponse{
		Offset:  offset,
		Message: message,
	}, nil
}

func decodeWriteResponse(r io.Reader) (*WriteResponse, error) {
	// Decoding offset
	var offset int64
	err := binary.Read(r, NetworkOrder, &offset)
	if err != nil {
		return nil, err
	}

	return &WriteResponse{
		Offset: offset,
	}, nil
}

func decodeErrorResponse(r io.Reader) (*ErrorResponse, error) {
	// Encoding message
	var n uint32
	err := binary.Read(r, NetworkOrder, &n)
	if err != nil {
		return nil, err
	}

	message := make([]byte, n)
	err = binary.Read(r, NetworkOrder, message)
	if err != nil {
		return nil, err
	}

	return &ErrorResponse{
		Message: string(message),
	}, nil
}

func EncodeResponse(w io.Writer, res Response) error {
	responseType := res.GetType()
	err := binary.Write(w, NetworkOrder, &responseType)
	if err != nil {
		return err
	}

	switch responseType {
	case RESPONSE_METADATA:
		return encodeMetadataResponse(w, res.(*MetaDataResponse))

	case RESPONSE_READ:
		return encodeReadResponse(w, res.(*ReadResponse))

	case RESPONSE_WRITE:
		return encodeWriteResponse(w, res.(*WriteResponse))

	case RESPONSE_ERROR:
		return encodeErrorResponse(w, res.(*ErrorResponse))

	default:
		return ErrUnknownResponseType
	}
}

func encodeMetadataResponse(io.Writer, *MetaDataResponse) error {
	return nil
}

func encodeReadResponse(w io.Writer, res *ReadResponse) error {
	// Encoding offset
	err := binary.Write(w, NetworkOrder, res.Offset)
	if err != nil {
		return err
	}

	// Encoding message
	err = binary.Write(w, NetworkOrder, uint32(len(res.Message)))
	if err != nil {
		return err
	}

	err = binary.Write(w, NetworkOrder, res.Message)
	if err != nil {
		return err
	}
	return nil
}

func encodeWriteResponse(w io.Writer, res *WriteResponse) error {
	// Encoding offset
	err := binary.Write(w, NetworkOrder, res.Offset)
	if err != nil {
		return err
	}

	return nil
}

func encodeErrorResponse(w io.Writer, res *ErrorResponse) error {
	// Encoding message
	message := []byte(res.Message)
	err := binary.Write(w, NetworkOrder, uint32(len(message)))
	if err != nil {
		return err
	}

	err = binary.Write(w, NetworkOrder, message)
	if err != nil {
		return err
	}

	return nil
}
