package consensus

import (
	"fmt"
)

// returned if a request times out
type TimeoutError struct {
	message string
}

func (e TimeoutError) Error() string  { return e.message }
func (e TimeoutError) String() string { return e.message }
func NewTimeoutError(format string, a ...interface{}) TimeoutError {
	return TimeoutError{fmt.Sprintf(format, a...)}
}

// returned if a request fails due to
// the request ballot being old
type BallotError struct {
	message string
}

func (e BallotError) Error() string  { return e.message }
func (e BallotError) String() string { return e.message }
func NewBallotError(format string, a ...interface{}) BallotError {
	return BallotError{fmt.Sprintf(format, a...)}
}

// returned if a request is aborted because
// it's no longer valid (ie: running a prepare phase on a committed
// instance)
type InvalidStatusUpdateError struct {
	InstanceID InstanceID
	CurrentStatus InstanceStatus
	ProposedStatus InstanceStatus
}

func (e InvalidStatusUpdateError) Error() string  { return e.String() }

func (e InvalidStatusUpdateError) String() string {
	return fmt.Sprintf(
		"Cannot update status from %v to %v for instance %v",
		e.CurrentStatus,
		e.ProposedStatus,
		e.InstanceID,
	)
}

func NewInvalidStatusUpdateError(instance *Instance, update InstanceStatus) InvalidStatusUpdateError {
	return InvalidStatusUpdateError{
		InstanceID: instance.InstanceID,
		CurrentStatus: instance.Status,
		ProposedStatus: update,
	}
}
