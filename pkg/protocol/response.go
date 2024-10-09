package protocol

import (
	"encoding/binary"
	"io"
)

type ResponseType uint32

const (
	REPONSE_METADATA ResponseType = iota
	REPONSE_READ     ResponseType = iota
	REPONSE_WRITE    ResponseType = iota
)

type Response interface {
	GetType() ResponseType
}

type MetaDataResponse struct {
}

func (*MetaDataResponse) GetType() ResponseType {
	return REPONSE_METADATA
}

type ReadResponse struct {
	Offset  uint64
	Message []byte
}

func (*ReadResponse) GetType() ResponseType {
	return REPONSE_READ
}

type WriteResponse struct {
	Offset uint64
}

func (*WriteResponse) GetType() ResponseType {
	return REPONSE_WRITE
}

func DecodeResponse(r io.Reader) (Response, error) {
	var responseType ResponseType
	err := binary.Read(r, NetworkOrder, &responseType)
	if err != nil {
		return nil, err
	}

	switch responseType {
	case REPONSE_METADATA:
		return decodeMetadataResponse(r)

	case REPONSE_READ:
		return decodeReadResponse(r)

	case REPONSE_WRITE:
		return decodeWriteResponse(r)

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
	var offset uint64
	err := binary.Read(r, NetworkOrder, &offset)
	if err != nil {
		return nil, err
	}

	return &WriteResponse{
		Offset: offset,
	}, nil
}

func EncodeResponse(w io.Writer, res Response) error {
	responseType := res.GetType()
	err := binary.Write(w, NetworkOrder, &responseType)
	if err != nil {
		return err
	}

	switch responseType {
	case REPONSE_METADATA:
		return encodeMetadataResponse(w, res.(*MetaDataResponse))

	case REPONSE_READ:
		return encodeReadResponse(w, res.(*ReadResponse))

	case REPONSE_WRITE:
		return encodeWriteResponse(w, res.(*WriteResponse))

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
