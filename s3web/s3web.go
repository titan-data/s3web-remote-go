/*
 * Copyright The Titan Project Contributors.
 */
package s3web

import (
	"errors"
	"fmt"
	"github.com/titan-data/remote-sdk-go/pkg/remote"
	"net/url"
	"reflect"
)

type s3webRemote struct {
}

func (s s3webRemote) Type() string {
	return "s3web"
}

func (s s3webRemote) FromURL(url url.URL, additionalProperties map[string]string) (map[string]interface{}, error) {
	// nop remotes can only be "nop", which means everything other than "path" must be empty
	if url.Scheme != "" || url.Host != "" || url.User != nil || url.Path != "nop" {
		return nil, errors.New("malformed remote identifier")
	}

	if len(additionalProperties) != 0 {
		return nil, errors.New(fmt.Sprintf("invalid property '%s'", reflect.ValueOf(additionalProperties).MapKeys()[0].String()))
	}

	return map[string]interface{}{}, nil
}

func (s s3webRemote) ToURL(properties map[string]interface{}) (string, map[string]string, error) {
	return "s3web", map[string]string{}, nil
}

func (s s3webRemote) GetParameters(remoteProperties map[string]interface{}) (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

func init() {
	remote.Register(s3webRemote{})
}
