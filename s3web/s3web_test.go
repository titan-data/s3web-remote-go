/*
 * Copyright The Titan Project Contributors.
 */
package s3web

import (
	"github.com/stretchr/testify/assert"
	"github.com/titan-data/remote-sdk-go/remote"
	"testing"
)

func TestRegistered(t *testing.T) {
	r := remote.Get("s3web")
	ret, err := r.Type()
	if assert.NoError(t, err) {
		assert.Equal(t, "s3web", ret)
	}
}

func TestFromURL(t *testing.T) {
	r := remote.Get("s3web")
	props, err := r.FromURL("s3web://host/object/path", map[string]string{})
	if assert.NoError(t, err) {
		assert.Equal(t, "http://host/object/path", props["url"])
	}
}

func TestNoPath(t *testing.T) {
	r := remote.Get("s3web")
	props, err := r.FromURL("s3web://host", map[string]string{})
	if assert.NoError(t, err) {
		assert.Equal(t, "http://host", props["url"])
	}
}

func TestBadProperty(t *testing.T) {
	r := remote.Get("s3web")
	_, err := r.FromURL("s3web://host", map[string]string{"a": "b"})
	assert.Error(t, err)
}

func TestBadScheme(t *testing.T) {
	r := remote.Get("s3web")
	_, err := r.FromURL("s3web", map[string]string{})
	assert.Error(t, err)
}

func TestBadSchemeName(t *testing.T) {
	r := remote.Get("s3web")
	_, err := r.FromURL("foo://bar", map[string]string{})
	assert.Error(t, err)
}

func TestBadUser(t *testing.T) {
	r := remote.Get("s3web")
	_, err := r.FromURL("s3web://user@host/path", map[string]string{})
	assert.Error(t, err)
}

func TestBadUserPassword(t *testing.T) {
	r := remote.Get("s3web")
	_, err := r.FromURL("s3web://user:password@host/path", map[string]string{})
	assert.Error(t, err)
}

func TestBadNoHost(t *testing.T) {
	r := remote.Get("s3web")
	_, err := r.FromURL("s3web:///path", map[string]string{})
	assert.Error(t, err)
}

func TestPort(t *testing.T) {
	r := remote.Get("s3web")
	props, err := r.FromURL("s3web://host:1023/object/path", map[string]string{})
	if assert.NoError(t, err) {
		assert.Equal(t, "http://host:1023/object/path", props["url"])
	}
}

func TestToURL(t *testing.T) {
	r := remote.Get("s3web")
	u, props, err := r.ToURL(map[string]interface{}{"url": "http://host/path"})
	if assert.NoError(t, err) {
		assert.Equal(t, "s3web://host/path", u)
		assert.Empty(t, props)
	}
}

func TestParameters(t *testing.T) {
	r := remote.Get("s3web")
	props, err := r.GetParameters(map[string]interface{}{"url": "http://host/path"})
	if assert.NoError(t, err) {
		assert.Empty(t, props)
	}
}
