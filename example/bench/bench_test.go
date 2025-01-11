package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/stretchr/testify/require"
	"io"
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

	b.RunParallel(func(pb *testing.PB) {
		cli := http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			}}}
		for pb.Next() {
			nanos := time.Now().UnixNano()
			data := fmt.Sprintf("{\"key\": \"%d\", \"value\": \"val_%d\"}", nanos, nanos)
			resp, err := cli.Post(*postURL, contentType, strings.NewReader(data))
			require.NoError(b, err, b.N)
			require.Equal(b, http.StatusCreated, resp.StatusCode)
		}
	})
	b.StopTimer()
	resp, err := http.Get(*getURL + "/state")
	require.NoError(b, err, b.N)
	data, err := io.ReadAll(resp.Body)
	require.NoError(b, err, b.N)
	var count int64
	err = json.Unmarshal(data, &count)
	require.NoError(b, err, b.N)
	b.ReportMetric(float64(count)/b.Elapsed().Seconds(), "creates/s")
}

func init() {
	postURL = flag.String("post-url", "", "POST URL to set values")
	getURL = flag.String("get-url", "", "GET URL to read values")
}
