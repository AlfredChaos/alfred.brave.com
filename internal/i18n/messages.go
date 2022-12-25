package i18n

const (
	ErrUnexpected Message = iota + 1
	ErrBadRequest

	MsgUserRegistered
)

var Messages = MessageMap{
	// Error messages:
	ErrUnexpected: "Unexpected error, please try again",
	ErrBadRequest: "Invalid request",

	MsgUserRegistered: "User %s register success.",
}
