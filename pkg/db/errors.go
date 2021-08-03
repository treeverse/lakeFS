package db

import (
	"errors"
	"fmt"
	"net"

	"github.com/jackc/pgx/v4"
)

var (
	ErrNotFound      = fmt.Errorf("not found: %w", pgx.ErrNoRows)
	ErrAlreadyExists = errors.New("already exists")
	ErrSerialization = errors.New("serialization error")
)

func IsDialError(err error) bool {
	netError := &net.OpError{}
	return errors.As(err, &netError) && netError.Op == "dial"
}
