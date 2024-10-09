package protocol

import "errors"

var (
	ErrUnknownRequestType  = errors.New("unknown request type")
	ErrUnknownResponseType = errors.New("unknown response type")
)
