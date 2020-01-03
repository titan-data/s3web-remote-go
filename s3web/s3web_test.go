/*
 * Copyright The Titan Project Contributors.
 */
package s3web

import (
	"github.com/stretchr/testify/assert"
	"github.com/titan-data/remote-sdk-go/pkg/remote"
	"testing"
)

func TestRegistered(t *testing.T) {
	r := remote.Get("s3web")
	assert.Equal(t, "s3web", r.Type())
}

