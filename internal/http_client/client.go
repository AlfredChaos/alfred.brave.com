package http_client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"

	"alfred.brave.com/event"
	"github.com/goccy/go-json"
	uuid "github.com/satori/go.uuid"
	"moul.io/http2curl"
)

var log = event.Log

type Client interface {
	Do(req *http.Request) (*http.Response, error)
}

type Headers struct {
	Key   string
	Value string
}

type Query struct {
	Key   string
	Value string
}

type URL struct {
	Base          string
	Path          string
	PathVariables []string
	QueryParams   []Query
}

var HTTPClient Client = &http.Client{}

func (u *URL) Concat() (string, error) {
	var fullUrl string
	var newUrl []string
	var query url.Values
	errMsg := errors.New("url concat error")
	if u.Base == "" || u.Path == "" {
		log.Errorf("url got nil")
		return "", errMsg
	}
	urlBase, _ := url.Parse(u.Base)
	newUrl = append(newUrl, urlBase.Path)
	pathVariablesCount := strings.Count(u.Base, "%s")
	if pathVariablesCount != len(u.PathVariables) {
		log.Errorf("url path_variables count incorrect")
		return u.Base, errMsg
	}
	if pathVariablesCount == 1 {
		fullUrl = fmt.Sprintf(u.Base, u.PathVariables[0])
	}
	if pathVariablesCount == 2 {
		fullUrl = fmt.Sprintf(u.Base, u.PathVariables[0], u.PathVariables[2])
	}
	if pathVariablesCount > 2 {
		log.Errorf("url length do not support")
		return "", errMsg
	}
	newUrl = append(newUrl, fullUrl)
	urlBase.Path = path.Join(newUrl...)
	fullUrl, _ = url.PathUnescape(urlBase.String())
	if len(u.QueryParams) == 0 {
		return fullUrl, nil
	}
	for _, q := range u.QueryParams {
		query.Set(q.Key, q.Value)
	}
	fullUrl = fmt.Sprintf("%s?%s", fullUrl, query.Encode())
	return fullUrl, nil
}

func Get(c *context.Context, url *URL, resp interface{}, headers ...Headers) (*http.Response, error) {
	return request(c, http.MethodGet, url, nil, resp, headers...)
}

func Post(c *context.Context, url *URL, body []byte, resp interface{}, headers ...Headers) (*http.Response, error) {
	return request(c, http.MethodPost, url, body, resp, headers...)
}

func Delete(c *context.Context, url *URL, resp interface{}, headers ...Headers) (*http.Response, error) {
	return request(c, http.MethodDelete, url, nil, resp, headers...)
}

func Put(c *context.Context, url *URL, body []byte, resp interface{}, headers ...Headers) (*http.Response, error) {
	return request(c, http.MethodPut, url, body, resp, headers...)
}

func request(c *context.Context, method string, u *URL, body []byte, resp interface{}, headers ...Headers) (*http.Response, error) {
	var req *http.Request
	errMsg := fmt.Errorf("error occurred wheile requesting")
	requestId := fmt.Sprintf("req-%s", uuid.NewV4().String())
	if u == nil {
		log.Errorf("[X-Request-ID: %s] url got nil", requestId)
		return nil, errMsg
	}
	fullUrl, err := u.Concat()
	if err != nil {
		log.Errorf("[X-Request-ID: %s] url error = %v", requestId, err)
		return nil, err
	}
	req, err = http.NewRequestWithContext(*c, method, fullUrl, bytes.NewBuffer(body))
	if err != nil {
		log.Errorf("[X-Request-ID: %s] generate request fail, error = %v", requestId, err)
		return nil, err
	}
	if body != nil {
		req.Header.Set(HeaderContentType, TypeJSON)
	}
	if len(headers) > 0 {
		for _, h := range headers {
			req.Header.Set(h.Key, h.Value)
		}
	}
	curl, err := http2curl.GetCurlCommand(req)
	if err != nil {
		log.Errorf("[X-Request-ID: %s] get full url fail, error = %v", requestId, err)
		return nil, err
	}
	log.Infof("[X-Request-ID: %s] [HTTP Request] %s", requestId, curl.String())
	response, err := HTTPClient.Do(req)
	if err != nil {
		log.Errorf("[X-Request-ID: %s] do request fail, error = %v", requestId, err)
		return nil, err
	}
	defer response.Body.Close()
	if strings.HasPrefix(response.Status, "4") {
		log.Errorf("[X-Request-ID: %s] get status %v error", requestId, response.Status)
		errMsg = fmt.Errorf("do request got %v error", response.Status)
		return nil, errMsg
	}
	if strings.HasPrefix(response.Status, "5") {
		log.Errorf("[X-Request-ID: %s] get status %v error", requestId, response.Status)
		errMsg = fmt.Errorf("do request got %v error", response.Status)
		return nil, errMsg
	}
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Errorf("[X-Request-ID: %s] read response body error = %v", requestId, err)
		return nil, err
	}
	if resp != nil {
		if err = json.Unmarshal(b, resp); err != nil {
			log.Errorf("[X-Request-ID: %s] unmarshal response body error = %v", requestId, err)
			return nil, err
		}
	}
	return response, nil
}
