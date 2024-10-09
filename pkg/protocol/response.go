package protocol

import "io"

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
}

func (*ReadResponse) GetType() ResponseType {
	return REPONSE_READ
}

type WriteResponse struct {
}

func (*WriteResponse) GetType() ResponseType {
	return REPONSE_WRITE
}

func DecodeResponse(r io.Reader) (Response, error) {
	return nil, nil
}

func EncodeResponse(w io.Writer, res Response) error {
	return nil
}
