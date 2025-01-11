package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/stretchr/testify/require"
	"net/http"
	"strings"
	"testing"
	"time"
)

var (
	postURL *string
	getURL  *string
)

const (
	contentType = "application/json"
)

func BenchmarkPut(b *testing.B) {
	cli := http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		}}}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			nanos := time.Now().UnixNano()
			data := fmt.Sprintf("{\"key\": \"%d\", \"value\": \"val_%d\"}", nanos, nanos)
			fmt.Println("posting ", data)
			resp, err := cli.Post(*postURL, contentType, strings.NewReader(data))
			require.NoError(b, err)
			require.Equal(b, http.StatusCreated, resp.StatusCode)
		}
	})
}

func init() {
	postURL = flag.String("post-url", "", "POST URL to set values")
	getURL = flag.String("get-url", "", "GET URL to read values")
}
