package httperr

import "fmt"

var PermanentErrCodes = []int{
	400, // Bad Request
	401, // Unauthorised
	403, // Forbidden
	404, // Not Found
	405, // Method Not Allowed
	406, // Not Acceptable
	410, // Gone
	451, // Unavailable For Legal Reasons
}

type Err struct {
	StatusCode int
}

func (e *Err) Error() string {
	return fmt.Sprintf("unexpected status: %d", e.StatusCode)
}
