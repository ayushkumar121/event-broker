package protocol

import (
  "io"
  "errors"
  "encoding/binary"
)

type RequestType int
const (
  REQUEST_METADATA RequestType = iota
  REQUEST_READ RequestType = iota
  REQUEST_WRITE RequestType = iota
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
  Topic string
  Partition int
}

func (*ReadRequest) GetType() RequestType {
  return REQUEST_READ
}

type WriteRequest struct {
  Topic string
  Partition int
  Message []byte
}

func (*WriteRequest) GetType() RequestType {
  return REQUEST_WRITE
}

var (
  ErrUnknownRequestType = errors.New("unknown request type")
)

func DecodeRequest(r io.Reader) (Request, error) {
  buf := make([]byte, 4)
  _, err := r.Read(buf)
  if err != nil {
    return nil, err
  }
  requestType := RequestType(binary.BigEndian.Uint32(buf))
  
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

func decodeMetadataRequest(r io.Reader) (*MetaDataRequest, error) {
  return nil, nil
}

func decodeReadRequest(r io.Reader) (*ReadRequest, error) {
  return nil, nil
}

func decodeWriteRequest(r io.Reader) (*WriteRequest, error) {
  return nil, nil
}

func EncodeRequest(req Request, w io.Writer) error {
  
}
