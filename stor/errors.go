package stor

import (
	"encoding/json"
	"fmt"
)

var (
	ErrArchiveNotFound    = fmt.Errorf("archive not found")
	ErrBucketNotEmpty     = fmt.Errorf("bucket not empty")
	ErrInvalidCredentials = fmt.Errorf("invalid credentials")
	ErrObjectNotFound     = fmt.Errorf("object not found")
	ErrNoSuchBucket       = fmt.Errorf("no such bucket")
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
	case "NoSuchArchive":
		return ErrArchiveNotFound, true
	case "NoSuchBucket":
		return ErrNoSuchBucket, true
	case "BucketNotEmpty":
		return ErrBucketNotEmpty, true
	}
	return nil, false
}
