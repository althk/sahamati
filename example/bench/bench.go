package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/stretchr/testify/require"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

const (
	contentType = "application/json"
)

func runBenchmark(b *testing.B) {
	cli := http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		}}}
	var i int64

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			atomic.AddInt64(&i, 1)
			resp, err := cli.Post(*postURL, contentType, strings.NewReader(
				fmt.Sprintf("{\"key\": \"key%d\", \"value\": \"value%d\"}", i, i)))
			require.NoError(b, err)
			if err != nil || resp != nil && resp.StatusCode != 201 {
				fmt.Println(resp.Status, i)
			}
		}
	})
	b.ReportMetric(float64(i)/float64(b.Elapsed().Nanoseconds()), "posts/ns")
}

var (
	postURL = flag.String("post-url", "", "POST URL to set values")
	getURL  = flag.String("get-url", "", "GET URL to read values")
)

func main() {
	flag.Parse()
	testing.Benchmark(runBenchmark)
}
