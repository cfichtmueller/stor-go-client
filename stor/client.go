// Copyright 2024 Christoph Fichtmüller. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package stor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	httpClient *http.Client
	host       string
	auth       string
}

// NewClient creates a new client to connect to a STOR server.
//
// When providing ClientOptions, only the first element will be used.
func NewClient(opts ...*ClientOptions) *Client {
	var opt *ClientOptions
	if len(opts) > 0 {
		opt = opts[0]
	} else {
		opt = NewClientOptions()
	}

	client := &Client{
		host:       opt.Host,
		auth:       "Bearer " + opt.ApiKey,
		httpClient: opt.HTTPCLient,
	}

	if opt.Timeout != nil {
		client.httpClient.Timeout = *opt.Timeout
	} else {
		client.httpClient.Timeout = 30 * time.Second
	}

	return client
}

func (c *Client) doReq(ctx context.Context, method, path, contentType string, body io.Reader) (*http.Response, []byte, error) {
	u := fmt.Sprintf("%s/%s", c.host, path)
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Add("Authorization", c.auth)
	if contentType != "" {
		req.Header.Add("Content-Type", contentType)
	}
	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, nil, err
	}
	return res, b, nil
}

type ClientOptions struct {
	Host       string
	ApiKey     string
	HTTPCLient *http.Client
	Timeout    *time.Duration
	err        error
}

func NewClientOptions() *ClientOptions {
	return &ClientOptions{
		HTTPCLient: http.DefaultClient,
	}
}

// SetHost sets the host of the STOR server.
func (c *ClientOptions) SetHost(host string) *ClientOptions {
	c.Host = host
	return c
}

// SetApiKey sets the API key to use for authentication.
func (c *ClientOptions) SetApiKey(apiKey string) *ClientOptions {
	c.ApiKey = apiKey
	return c
}

// SetTimeout specifies a timeout that is used for creating connections to the server.
// If set to 0, no timeout will be used. The default is 30 seconds.
func (c *ClientOptions) SetTimout(timeout time.Duration) *ClientOptions {
	c.Timeout = &timeout
	return c
}

// Validate validates the client options. This method will return the first error found.
func (c *ClientOptions) Validate() error {
	if c.err != nil {
		return c.err
	}
	c.err = c.validate()
	return c.err
}

func (c *ClientOptions) validate() error {
	if c.Host == "" {
		return errors.New("a host is required")
	}
	if c.ApiKey == "" {
		return errors.New("an API key is required")
	}

	return nil
}
