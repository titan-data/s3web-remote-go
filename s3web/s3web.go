/*
 * Copyright The Titan Project Contributors.
 */
package s3web

import (
	"errors"
	"fmt"
	"github.com/titan-data/remote-sdk-go/remote"
	"net/url"
	"reflect"
	"strings"
)

type s3webRemote struct {
}

func (s s3webRemote) Type() (string, error) {
	return "s3web", nil
}

func (s s3webRemote) FromURL(rawUrl string, additionalProperties map[string]string) (map[string]interface{}, error) {
	u, err := url.Parse(rawUrl)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "s3web" {
		return nil, errors.New("invalid remote scheme")
	}

	if u.User != nil {
		return nil, errors.New("remote username and password cannot be specified")
	}

	if u.Hostname() == "" {
		return nil, errors.New("missing remote host name")
	}

	if len(additionalProperties) != 0 {
		return nil, errors.New(fmt.Sprintf("invalid property '%s'", reflect.ValueOf(additionalProperties).MapKeys()[0].String()))
	}

	res := fmt.Sprintf("http://%s%s", u.Host, u.Path)
	return map[string]interface{}{"url": res}, nil
}

func (s s3webRemote) ToURL(properties map[string]interface{}) (string, map[string]string, error) {
	u := properties["url"].(string)
	return strings.Replace(u, "http", "s3web", 1), map[string]string{}, nil
}

func (s s3webRemote) GetParameters(remoteProperties map[string]interface{}) (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

func (s s3webRemote) ValidateRemote(properties map[string]interface{}) error {
	panic("implement me")
}

func (s s3webRemote) ValidateParameters(parameters map[string]interface{}) error {
	panic("implement me")
}

func (s s3webRemote) ListCommits(properties map[string]interface{}, parameters map[string]interface{}, tags []remote.Tag) ([]remote.Commit, error) {
	panic("implement me")
}

func (s s3webRemote) GetCommit(properties map[string]interface{}, parameters map[string]interface{}, commitId string) (*remote.Commit, error) {
	panic("implement me")
}

func init() {
	remote.Register(s3webRemote{})
}
