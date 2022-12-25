package i18n

type Response struct {
	Code int    `json:"code"`
	Err  string `json:"error,omitempty"`
	Msg  string `json:"message,omitempty"`
}

func (r Response) String() string {
	if r.Err != "" {
		return r.Err
	} else {
		return r.Msg
	}
}

func NewResponse(code int, id Message, params ...interface{}) Response {
	if code < 400 {
		return Response{Code: code, Msg: Msg(id, params...)}
	} else {
		return Response{Code: code, Err: Msg(id, params...)}
	}
}
