package protocol

import "io"

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

func ParseRequest(r io.Reader) (Request, error) {
  
}
