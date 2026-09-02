package i18n

const (
	ErrUnexpected Message = iota + 1
	ErrBadRequest
	ErrNotFound
	ErrPassword
	ErrDatabase
	ErrLogin
	ErrUnauthorized
	ErrServiceUnavailable
	ErrForbidden
	ErrConflict

	MsgUserRegistered
	MsgUserLogin
)

var Messages = MessageMap{
	// Error messages:
	ErrUnexpected:         "Unexpected error, please try again",
	ErrBadRequest:         "Invalid request",
	ErrNotFound:           "Not found",
	ErrPassword:           "Wrong password",
	ErrDatabase:           "Database error",
	ErrLogin:              "User or email wrong, please try again",
	ErrUnauthorized:       "Unauthorized: invalid or expired token",
	ErrServiceUnavailable: "Service temporarily unavailable",
	ErrForbidden:          "Forbidden: owner permission required",
	ErrConflict:           "Conflict with current state",

	MsgUserRegistered: "User %s register success.",
	MsgUserLogin:      "User %s login success.",
}
