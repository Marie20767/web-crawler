package httperr

import "fmt"

type Err struct {
	StatusCode int
}

func (e *Err) Error() string {
	return fmt.Sprintf("unexpected status: %d", e.StatusCode)
}