package stor

import (
	"encoding/json"
	"fmt"
)

var (
	ErrInvalidCredentials = fmt.Errorf("invalid credentials")
	ErrObjectNotFound     = fmt.Errorf("object not found")
)

type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func mapErrorResponse(body []byte) (error, bool) {
	var er ErrorResponse
	if err := json.Unmarshal(body, &er); err != nil {
		return nil, false
	}
	switch er.Code {
	case "InvalidCredentials":
		return ErrInvalidCredentials, true
	}
	return nil, false
}
