package i18n

const (
	ErrUnexpected Message = iota + 1
	ErrBadRequest
	ErrNotFound
	ErrPassword
	ErrDatabase
	ErrLogin

	MsgUserRegistered
	MsgUserLogin
)

var Messages = MessageMap{
	// Error messages:
	ErrUnexpected: "Unexpected error, please try again",
	ErrBadRequest: "Invalid request",
	ErrNotFound:   "Not found",
	ErrPassword:   "Wrong password",
	ErrDatabase:   "Database error",
	ErrLogin:      "User or email wrong, please try again",

	MsgUserRegistered: "User %s register success.",
	MsgUserLogin:      "User %s login success.",
}
