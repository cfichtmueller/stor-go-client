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
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type Object struct {
	Key         string    `json:"key"`
	ContentType string    `json:"contentType"`
	Size        uint64    `json:"size"`
	CreatedAt   time.Time `json:"createdAt"`
}

type ObjectReference struct {
	Key string `json:"key"`
}

type Error struct {
	Code    string
	Message string
}

type CreateObjectCommand struct {
	Bucket      string
	Key         string
	ContentType string
	Data        io.Reader
	// IfNoneMatch uploads the object only if the object key name does not already exist in the bucket
	IfNoneMatch bool
}

type CreateObjectResult struct {
	ETag string `json:"etag"`
}

func (c *Client) CreateObject(ctx context.Context, cmd CreateObjectCommand) (*CreateObjectResult, error) {
	header := http.Header{}
	if cmd.IfNoneMatch {
		header.Set("If-None-Match", "*")
	}
	res, _, err := c.doReq(ctx, R{
		method:      "PUT",
		path:        objectPath(cmd.Bucket, cmd.Key),
		header:      header,
		contentType: cmd.ContentType,
		body:        cmd.Data,
	})
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 204 {
		//TODO: map error
		return nil, fmt.Errorf("unable to create object: %v", res.StatusCode)
	}

	return &CreateObjectResult{
		ETag: res.Header.Get("ETag"),
	}, nil
}

type CreateMultipartUploadCommand struct {
	Bucket      string
	Key         string
	ContentType string
}

type CreateMultipartUploadResult struct {
	Bucket   string
	Key      string
	UploadId string
}

// CreateMultipartUpload initiates a multipart upload.
func (c *Client) CreateMultipartUpload(ctx context.Context, cmd CreateMultipartUploadCommand) (*CreateMultipartUploadResult, error) {
	query := url.Values{}
	query.Set("uploads", "")
	res, body, err := c.doReq(ctx, R{
		method:      "POST",
		path:        objectPath(cmd.Bucket, cmd.Key),
		query:       query,
		contentType: cmd.ContentType,
	})
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		//TODO: map error
		return nil, fmt.Errorf("unable to create multipart upload: %v", res.StatusCode)
	}

	var result CreateMultipartUploadResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

type UploadPartCommand struct {
	Bucket        string
	Key           string
	UploadId      string
	PartNumber    int
	Data          io.Reader
	ContentLength int
}

type UploadPartResponse struct {
	ETag string
}

// UploadPart uploads a part in a multipart upload.
func (c *Client) UploadPart(ctx context.Context, cmd UploadPartCommand) (*UploadPartResponse, error) {
	query := url.Values{}
	query.Set("upload-id", cmd.UploadId)
	query.Set("part-number", strconv.Itoa(cmd.PartNumber))
	res, _, err := c.doReq(ctx, R{
		method:        "PUT",
		path:          objectPath(cmd.Bucket, cmd.Key),
		query:         query,
		contentLength: cmd.ContentLength,
	})
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		//TODO: map error
		return nil, fmt.Errorf("unable to upload part: %v", res.StatusCode)
	}

	return &UploadPartResponse{
		ETag: res.Header.Get("ETag"),
	}, nil
}

type PartReference struct {
	ETag       string `json:"etag"`
	PartNumber int    `json:"partNumber"`
}

type CompleteMultipartUploadCommand struct {
	Bucket   string
	Key      string
	UploadId string
	// IfNoneMatch uploads the object only if the object key name does not already exist in the bucket
	IfNoneMatch bool
	Parts       []PartReference
}

type CompleteMultipartUploadResult struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	ETag   string `json:"etag"`
}

type completeMultipartUploadRequest struct {
	Parts []PartReference `json:"parts"`
}

func (c *Client) CompleteMultipartUpload(ctx context.Context, cmd CompleteMultipartUploadCommand) (*CompleteMultipartUploadResult, error) {
	query := url.Values{}
	query.Set("upload-id", cmd.UploadId)
	header := http.Header{}
	if cmd.IfNoneMatch {
		header.Set("If-None-Match", "*")
	}
	body, err := json.Marshal(completeMultipartUploadRequest{
		Parts: cmd.Parts,
	})
	if err != nil {
		return nil, err
	}
	res, responseBody, err := c.doReq(ctx, R{
		method: "POST",
		path:   objectPath(cmd.Bucket, cmd.Key),
		query:  query,
		header: header,
		body:   bytes.NewBuffer(body),
	})
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		//TODO: map error
		return nil, fmt.Errorf("unable to complete upload: %v", res.StatusCode)
	}

	var result CompleteMultipartUploadResult
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

type AbortMultipartUploadCommand struct {
	Bucket   string
	Key      string
	UploadId string
}

func (c *Client) AbortMultipartUpload(ctx context.Context, cmd AbortMultipartUploadCommand) error {
	query := url.Values{}
	query.Set("upload-id", cmd.UploadId)
	res, _, err := c.doReq(ctx, R{
		method: "DELETE",
		path:   objectPath(cmd.Bucket, cmd.Key),
		query:  query,
	})
	if err != nil {
		return err
	}
	if res.StatusCode != 204 {
		return fmt.Errorf("unable to abort multipart upload: %d", res.StatusCode)
	}

	return nil
}

type ListObjectsCommand struct {
	Bucket     string
	StartAfter string
	// MaxKeys limits the results to max keys. Defaults to 1000. Max is 1000.
	MaxKeys   int
	Delimiter string
	Prefix    string
}

type ListObjectsResult struct {
	IsTruncated    bool      `json:"isTruncated"`
	Objects        []*Object `json:"objects"`
	Name           string    `json:"name"`
	MaxKeys        int       `json:"maxKeys"`
	KeyCount       int       `json:"keyCount"`
	StartAfter     *string   `json:"startAfter,omitempty"`
	CommonPrefixes []string  `json:"commonPrefixes,omitempty"`
}

func (c *Client) ListObjects(ctx context.Context, r ListObjectsCommand) (*ListObjectsResult, error) {
	maxKeys := r.MaxKeys
	if maxKeys < 1 {
		maxKeys = 1000
	}
	q := url.Values{}
	q.Add("start-after", r.StartAfter)
	q.Add("max-keys", strconv.Itoa(maxKeys))
	q.Add("delimiter", r.Delimiter)
	q.Add("prefix", r.Prefix)
	q.Encode()
	res, body, err := c.doReq(ctx, R{
		path:  r.Bucket,
		query: q,
	})
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("unable to list objects: %d", res.StatusCode)
	}
	var listResult ListObjectsResult
	if err := json.Unmarshal(body, &listResult); err != nil {
		return nil, fmt.Errorf("unable to unmarshal server response: %v", err)
	}
	return &listResult, nil
}

type ReadObjectResult struct {
	ContentType   string
	ContentLength int64
	body          io.ReadCloser
}

func (r *ReadObjectResult) Read(p []byte) (int, error) {
	return r.body.Read(p)
}

func (r *ReadObjectResult) Close() error {
	return r.body.Close()
}

// ReadObject reads an object from STOR.
// Clients are expected to read and close the returned ReadObjectResult.
// If the object cannot be found, the method returns ErrObjectNotFound.
func (c *Client) ReadObject(ctx context.Context, bucket, key string) (*ReadObjectResult, error) {
	req, err := c.createReq(ctx, R{
		path: bucket + "/" + key,
	})
	if err != nil {
		return nil, err
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode == 404 {
		return nil, ErrObjectNotFound
	}

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected status code: %v", res.StatusCode)
	}

	return &ReadObjectResult{
		ContentType:   res.Header.Get("Content-Type"),
		ContentLength: res.ContentLength,
		body:          res.Body,
	}, nil
}

type DeleteObjectsCommand struct {
	Bucket  string
	Objects []ObjectReference
}
type DeleteObjectsResult struct {
	Results []DeleteResult `json:"results"`
}

type DeleteResult struct {
	Key     string `json:"key"`
	Deleted bool   `json:"deleted"`
	Error   *Error `json:"error,omitempty"`
}

type deleteObjectsRequest struct {
	Objects []ObjectReference `json:"objects"`
}

func (c *Client) DeleteObjects(ctx context.Context, cmd DeleteObjectsCommand) (*DeleteObjectsResult, error) {
	data, err := json.Marshal(deleteObjectsRequest{Objects: cmd.Objects})
	if err != nil {
		return nil, err
	}
	query := url.Values{}
	query.Set("delete", "")
	res, body, err := c.doReq(ctx, R{
		method:      "POST",
		path:        cmd.Bucket,
		query:       query,
		contentType: "application/json",
		body:        bytes.NewReader(data),
	})
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("unable to delete objects: %d", res.StatusCode)
	}

	var result DeleteObjectsResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func objectPath(bucketName, key string) string {
	return bucketName + "/" + key
}
