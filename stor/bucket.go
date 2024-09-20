// Copyright 2024 Christoph Fichtmüller. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package stor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Bucket struct {
	Name      string    `json:"name"`
	Objects   uint64    `json:"objects"`
	Size      uint64    `json:"size"`
	CreatedAt time.Time `json:"createdAt"`
}

func (c *Client) CreateBucket(ctx context.Context, name string) (*Bucket, error) {
	res, body, err := c.doReq(ctx, "PUT", name, "", nil)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 201 {
		//TODO: map error
		return nil, fmt.Errorf("unable to create bucket: %v", res.StatusCode)
	}
	var bucket Bucket
	if err := json.Unmarshal(body, &bucket); err != nil {
		return nil, err
	}

	return &bucket, nil
}
