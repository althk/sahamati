package kvstore_client

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/stretchr/testify/require"
	"net/http"
	"strings"
	"testing"
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

	for i := 0; i < b.N; i++ {
		resp, err := cli.Post(*postURL, contentType, strings.NewReader(
			fmt.Sprintf("{\"key\": \"key%d\", \"value\": \"value%d\"}", i, i)))
		require.NoError(b, err)
		require.Equal(b, http.StatusOK, resp.StatusCode)
	}
}

func init() {
	postURL = flag.String("post-url", "", "POST URL to set values")
	getURL = flag.String("get-url", "", "GET URL to read values")
	flag.Parse()
}
