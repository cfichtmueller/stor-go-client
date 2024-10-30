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
	"net/url"
	"strconv"
	"time"
)

type Client struct {
	httpClient *http.Client
	host       string
	auth       string
}

type R struct {
	method        string
	path          string
	query         url.Values
	contentType   string
	contentLength int
	body          io.Reader
	header        http.Header
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

func (c *Client) newUrl() *url.URL {
	u, err := url.Parse(c.host)
	if err != nil {
		panic(err)
	}
	return u
}

func (c *Client) createReq(ctx context.Context, r R) (*http.Request, error) {
	method := r.method
	if method == "" {
		method = "GET"
	}
	u := fmt.Sprintf("%s/%s", c.host, r.path)
	if len(r.query) > 0 {
		u = u + "?" + r.query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, r.body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", c.auth)
	if r.contentType != "" {
		req.Header.Add("Content-Type", r.contentType)
	}
	if r.contentLength != 0 {
		req.Header.Add("Content-Length", strconv.Itoa(r.contentLength))
	}

	if r.header != nil {
		for k, v := range r.header {
			for _, vv := range v {
				req.Header.Add(k, vv)
			}
		}
	}

	return req, nil
}

func (c *Client) doReq(ctx context.Context, r R) (*http.Response, []byte, error) {
	req, err := c.createReq(ctx, r)
	if err != nil {
		return nil, nil, err
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
