// ovirt-imageio
// Copyright (C) 2021 Red Hat, Inc.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation; either version 2 of the
// License, or (at your option) any later version.

package http

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"ovirt.org/imageio"
)

type backend struct {
	url     string
	client  *http.Client
	size    uint64
	extents []*imageio.Extent
}

// Connect to imageio http server.
func Connect(url string) (*backend, error) {
	tr := &http.Transport{
		// TODO: Support server certificate verification.
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},

		// Increass throughput from 400 MiB/s to 1.3 GiB/s
		// https://go-review.googlesource.com/c/go/+/76410.
		WriteBufferSize: 128 * 1024,

		// TODO: connection and read timeouts.
	}
	client := &http.Client{Transport: tr}

	// TODO: Send OPTIONS request

	return &backend{url: url, client: client}, nil
}

// Size return image size.
func (b *backend) Size() (uint64, error) {
	if b.size == 0 {
		// imageio does not expose the size of the image in the OPTIONS request
		// yet. The only way to get size is to get all the extents and compute
		// the size from the last extent.
		extents, err := b.Extents()
		if err != nil {
			return 0, err
		}
		last := extents[len(extents)-1]
		b.size = last.Start + last.Length
	}
	return b.size, nil
}

// Extents returns all image extents. Imageio server does not support getting
// partial extent yet.
func (b *backend) Extents() ([]*imageio.Extent, error) {
	if len(b.extents) == 0 {
		if err := b.getExtents(); err != nil {
			return nil, err
		}
	}
	return b.extents, nil
}

// Close closes the connection to imageio server.
func (b *backend) Close() {
	b.client.CloseIdleConnections()
}

func (b *backend) getExtents() error {
	res, err := b.client.Get(b.url + "/extents")
	if err != nil {
		return err
	}

	// We always want to read the entire response and close the body so we can
	// send a new request on the same connection.
	defer res.Body.Close()

	// If the response is an errror, the response body contains the error
	// message from the server.
	if res.StatusCode != 200 {
		reason, err := io.ReadAll(res.Body)
		if err != nil {
			reason = []byte(err.Error())
		}
		return fmt.Errorf("Cannot get extents: %s", reason)
	}

	// Successful response, read the json.
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("Cannot get extents: %s", err)
	}

	// Parse the json.
	err = json.Unmarshal(body, &b.extents)
	if err != nil {
		return fmt.Errorf("Cannot get extents: %s", err)
	}

	return nil
}
