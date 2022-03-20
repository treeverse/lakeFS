package application

import (
	"github.com/treeverse/lakefs/pkg/gateway/sig"
	"github.com/treeverse/lakefs/pkg/httputil"
	"net/http"
)

func NewLakeFsHttpServer(listenAddress string, s3GatewayDomainNames []string, s3gatewayHandler http.Handler, apiHandler http.Handler) *http.Server {
	return &http.Server{
		Addr: listenAddress,
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			// If the request has the S3 GW domain (exact or subdomain) - or carries an AWS sig, serve S3GW
			if httputil.HostMatches(request, s3GatewayDomainNames) ||
				httputil.HostSubdomainOf(request, s3GatewayDomainNames) ||
				sig.IsAWSSignedRequest(request) {
				s3gatewayHandler.ServeHTTP(writer, request)
				return
			}

			// Otherwise, serve the API handler
			apiHandler.ServeHTTP(writer, request)
		}),
	}

}
