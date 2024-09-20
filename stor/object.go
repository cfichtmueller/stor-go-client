// Copyright 2024 Christoph Fichtmüller. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package stor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type Object struct {
	Key         string    `json:"key"`
	ContentType string    `json:"contentType"`
	Size        uint64    `json:"size"`
	CreatedAt   time.Time `json:"createdAt"`
}

type BulkDeleteRequest struct {
	Objects []ObjectReference `json:"objects`
}

type ObjectReference struct {
	Key string `json:"key"`
}

type DeleteResults struct {
	Results []DeleteResult `json:"results"`
}

type DeleteResult struct {
	Key     string `json:"key"`
	Deleted bool   `json:"deleted"`
	Error   *Error `json:"error,omitempty"`
}

type Error struct {
	Code    string
	Message string
}

func (c *Client) CreateObject(ctx context.Context, bucketName, key, contentType string, data io.Reader) error {
	res, _, err := c.doReq(ctx, "PUT", fmt.Sprintf("%s/%s", bucketName, key), contentType, data)
	if err != nil {
		return err
	}
	if res.StatusCode != 204 {
		//TODO: map error
		return fmt.Errorf("unable to create object: %v", res.StatusCode)
	}
	return nil
}

func (c *Client) ListObjects(ctx context.Context, bucketName, startAfter string, limit int) ([]Object, error) {
	res, body, err := c.doReq(ctx, "GET", fmt.Sprintf("%s?start-after=%s&limit=%d", bucketName, startAfter, limit), "", nil)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("unable to list objects: %d", res.StatusCode)
	}
	var objects []Object
	if err := json.Unmarshal(body, &objects); err != nil {
		return nil, err
	}
	return objects, nil
}

func (c *Client) DeleteObjects(ctx context.Context, bucketName string, req BulkDeleteRequest) (DeleteResults, error) {

	data, err := json.Marshal(req)
	if err != nil {
		return DeleteResults{}, err
	}
	res, body, err := c.doReq(ctx, "POST", bucketName+"?delete", "application/json", bytes.NewReader(data))
	if err != nil {
		return DeleteResults{}, err
	}
	if res.StatusCode != 200 {
		return DeleteResults{}, fmt.Errorf("unable to delete objects: %d", res.StatusCode)
	}

	var results DeleteResults
	if err := json.Unmarshal(body, &results); err != nil {
		return DeleteResults{}, err
	}

	return results, nil
}
