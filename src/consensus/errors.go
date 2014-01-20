package consensus

import (
	"fmt"
)

type TimeoutError struct {
	message string
}

func (e TimeoutError) Error() string  { return e.message }
func (e TimeoutError) String() string { return e.message }
func NewTimeoutError(format string, a ...interface{}) TimeoutError {
	return TimeoutError{fmt.Sprintf(format, a...)}
}

type BallotError struct {
	message string
}

func (e BallotError) Error() string  { return e.message }
func (e BallotError) String() string { return e.message }
func NewBallotError(format string, a ...interface{}) BallotError {
	return BallotError{fmt.Sprintf(format, a...)}
}

