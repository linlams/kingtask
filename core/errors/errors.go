package errors

import (
	"errors"
)

func NewError(msg string) error {
	return errors.New(msg)
}

var (
	ErrMessageType     = errors.New("message type error")
	ErrInvalidArgument = errors.New("invalid argument")
	ErrTryMaxTimes     = errors.New("retry task max time")
	ErrFileNotExist    = errors.New("file not exist")
	ErrBadConn         = errors.New("bad net connection")
	ErrResultNotReady  = errors.New("result not ready")
	ErrExecTimeout     = errors.New("exec time out")
)
