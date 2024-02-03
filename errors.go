package bqs

import (
	"errors"
)

var (
	ErrConflictField       = errors.New("conflict field")
	ErrUnsupportedDataType = errors.New("unsupported data type")
	ErrUnsupportedObject   = errors.New("unsupported object, must be struct or map")
	ErrUnsupportedKeyType  = errors.New("unsupported map key type, must be string")
)
