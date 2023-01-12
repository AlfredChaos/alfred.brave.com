package http_client

import (
	"context"
	"net/http"
)

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
	PathVariables []string
	QueryParams   []Query
}

var HTTPClient Client = &http.Client{}

func Get(c *context.Context, url string, resp interface{}, headers ...Headers, query ...Query) (*http.Response, error) {
	return request(c, http.MethodGet, url, nil, resp, headers, query...)
}

func Post(c *context.Context, url string, body []byte, resp interface{}, headers ...Headers, query ...Query) (*http.Response, error) {
	return request(c, http.MethodPost, url, body, resp, headers, query...)
}

func Delete(c *context.Context, url string, resp interface{}, headers ...Headers, query ...Query) (*http.Response, error) {
	return request(c, http.MethodDelete, url, nil, resp, headers, query)
}

func Put(c *context.Context, url string, body []byte, resp interface{}, headers ...Headers, query ...Query) (*http.Response, error) {
	return request(c, http.MethodPut, url, body, resp, headers, query...)
}

func request(c *context.Context, method, url string, body []byte, resp interface{}, headers ...Headers, query ...Query) (*http.Response, error) {
	return nil, nil
}
