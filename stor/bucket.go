// Copyright 2024 Christoph Fichtmüller. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package stor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

type Bucket struct {
	Name      string    `json:"name"`
	Objects   int64     `json:"objects"`
	Size      int64     `json:"size"`
	CreatedAt time.Time `json:"createdAt"`
}

type ListBucketsCommand struct {
	StartAfter string
	MaxBuckets int
}

type ListBucketsResult struct {
	Buckets     []Bucket `json:"buckets"`
	IsTruncated bool     `json:"isTruncated"`
}

func (c *Client) ListBuckets(ctx context.Context, cmd ListBucketsCommand) (*ListBucketsResult, error) {
	query := url.Values{}
	if cmd.StartAfter != "" {
		query.Set("start-after", cmd.StartAfter)
	}
	if cmd.MaxBuckets != 0 {
		query.Set("max-buckets", strconv.Itoa(cmd.MaxBuckets))
	}
	res, body, err := c.doReq(ctx, R{})
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		err, ok := mapErrorResponse(body)
		if ok {
			return nil, err
		}
		//TODO: map error
		return nil, fmt.Errorf("unable to list buckets: %v", res.StatusCode)
	}
	var listResult ListBucketsResult
	if err := json.Unmarshal(body, &listResult); err != nil {
		return nil, fmt.Errorf("unable to unmarshal response: %v", err)
	}
	return &listResult, nil
}

type CreateBucketCommand struct {
	Name string
}

func (c *Client) CreateBucket(ctx context.Context, cmd CreateBucketCommand) (*Bucket, error) {
	res, body, err := c.doReq(ctx, R{
		method: "PUT",
		path:   cmd.Name,
	})
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 201 {
		err, ok := mapErrorResponse(body)
		if ok {
			return nil, err
		}
		//TODO: map error
		return nil, fmt.Errorf("unable to create bucket: %v", res.StatusCode)
	}
	var bucket Bucket
	if err := json.Unmarshal(body, &bucket); err != nil {
		return nil, fmt.Errorf("unable to unmarshal response: %v", err)
	}

	return &bucket, nil
}

type DeleteBucketCommand struct {
	Name string
}

func (c *Client) DeleteBucket(ctx context.Context, cmd DeleteBucketCommand) error {
	res, body, err := c.doReq(ctx, R{
		method: "DELETE",
		path:   cmd.Name,
	})
	if err != nil {
		return err
	}
	if res.StatusCode != 204 {
		err, ok := mapErrorResponse(body)
		if ok {
			return err
		}
		//TODO: map error
		return fmt.Errorf("unable to delete bucket: %v", res.StatusCode)
	}
	return nil
}
