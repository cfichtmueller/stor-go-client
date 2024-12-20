// Copyright 2024 Christoph Fichtmüller. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package stor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

var (
	ArchiveTypeZip         = "zip"
	ArchiveStatePending    = "pending"
	ArchiveStateProcessing = "processing"
	ArchiveStateComplete   = "complete"
	ArchiveStateFailed     = "failed"
	ErrArchiveNotFound     = fmt.Errorf("archive not found")
)

type CreateArchiveCommand struct {
	Bucket string
	Key    string
	Type   string
}

type CreateArchiveResult struct {
	Bucket    string
	Key       string
	ArchiveId string
}

// CreateArchive creates an archive.
func (c *Client) CreateArchive(ctx context.Context, cmd CreateArchiveCommand) (*CreateArchiveResult, error) {
	query := url.Values{}
	query.Set("archives", "")
	query.Set("type", cmd.Type)
	res, body, err := c.doReq(ctx, R{
		method: "POST",
		path:   objectPath(cmd.Bucket, cmd.Key),
		query:  query,
	})
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		//TODO: map error
		return nil, fmt.Errorf("unable to create archive: %v", res.StatusCode)
	}

	var result CreateArchiveResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

type ArchiveEntry struct {
	// Key is the key of the object to add to the archive
	Key string `json:"key"`
	// Name is the name of the resulting file
	Name string `json:"name"`
}

type AddArchiveEntriesCommand struct {
	Bucket    string
	Key       string
	ArchiveId string
	Entries   []ArchiveEntry
}

type addArchiveEntriesRequest struct {
	Entries []ArchiveEntry
}

// UploadPart uploads a part in a multipart upload.
func (c *Client) AddArchiveEntries(ctx context.Context, cmd AddArchiveEntriesCommand) error {
	query := url.Values{}
	query.Set("archive-id", cmd.ArchiveId)
	body, err := json.Marshal(addArchiveEntriesRequest{Entries: cmd.Entries})
	if err != nil {
		return err
	}
	res, _, err := c.doReq(ctx, R{
		method:      "PUT",
		path:        objectPath(cmd.Bucket, cmd.Key),
		query:       query,
		body:        bytes.NewReader(body),
		contentType: "application/json",
	})
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		//TODO: map error
		return fmt.Errorf("unable to add archive entries: %v", res.StatusCode)
	}

	return nil
}

type CompleteArchiveCommand struct {
	Bucket    string
	Key       string
	ArchiveId string
	// IfNoneMatch creates the archive only if the object key name does not already exist in the bucket
	IfNoneMatch bool
}

func (c *Client) CompleteArchive(ctx context.Context, cmd CompleteArchiveCommand) error {
	query := url.Values{}
	query.Set("archive-id", cmd.ArchiveId)
	header := http.Header{}
	if cmd.IfNoneMatch {
		header.Set("If-None-Match", "*")
	}
	res, _, err := c.doReq(ctx, R{
		method: "POST",
		path:   objectPath(cmd.Bucket, cmd.Key),
		query:  query,
		header: header,
	})
	if err != nil {
		return err
	}
	if res.StatusCode != 204 {
		//TODO: map error
		return fmt.Errorf("unable to complete archive: %v", res.StatusCode)
	}

	return nil
}

type AbortArchiveCommand struct {
	Bucket    string
	Key       string
	ArchiveId string
}

func (c *Client) AbortArchive(ctx context.Context, cmd AbortArchiveCommand) error {
	query := url.Values{}
	query.Set("archive-id", cmd.ArchiveId)
	res, _, err := c.doReq(ctx, R{
		method: "DELETE",
		path:   objectPath(cmd.Bucket, cmd.Key),
		query:  query,
	})
	if err != nil {
		return err
	}
	if res.StatusCode != 204 {
		return fmt.Errorf("unable to abort archive: %d", res.StatusCode)
	}

	return nil
}

type GetArchiveCommand struct {
	Bucket    string
	Key       string
	ArchiveId string
}

type GetArchiveResult struct {
	ID    string `json:"id"`
	State string `json:"state"`
	Type  string `json:"type"`
}

func (c *Client) GetArchive(ctx context.Context, cmd GetArchiveCommand) (*GetArchiveResult, error) {
	query := url.Values{}
	query.Set("archive-id", cmd.ArchiveId)
	res, body, err := c.doReq(ctx, R{
		method: "GET",
		path:   objectPath(cmd.Bucket, cmd.Key),
		query:  query,
	})
	if err != nil {
		return nil, err
	}
	if res.StatusCode == 404 {
		return nil, ErrArchiveNotFound
	} else if res.StatusCode != 200 {
		//TODO: map error
		return nil, fmt.Errorf("unable to get archive: %v", res.StatusCode)
	}

	var result GetArchiveResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("unable to unmarshal server response: %v", err)
	}

	return &result, nil
}
