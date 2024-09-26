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

type CreateNonceCommand struct {
	Bucket string
	Key    string
	TTL    time.Duration
}

type CreateNonceResult struct {
	Nonce     string    `json:"nonce"`
	ExpiresAt time.Time `json:"expiresAt"`
}

func (c *Client) CreateNonce(ctx context.Context, cmd CreateNonceCommand) (*CreateNonceResult, error) {
	query := url.Values{}
	query.Set("nonces", "")
	query.Set("ttl", strconv.Itoa(int(cmd.TTL.Seconds())))

	res, body, err := c.doReq(ctx, R{
		method: "POST",
		path:   objectPath(cmd.Bucket, cmd.Key),
		query:  query,
	})
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 201 {
		//TODO: map error
		return nil, fmt.Errorf("unable to create nonce: %v", res.StatusCode)
	}

	var result CreateNonceResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	return &result, nil
}
