package api

import (
	"context"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"

	"google.golang.org/grpc/status"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func AuthenticationStreamServerInterceptor(authenticator Authenticator) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// authenticate first
		ctx, err := authenticator.Authenticate(ss.Context())
		if err != nil {
			return err
		}
		wrapped := grpc_middleware.WrapServerStream(ss)
		wrapped.WrappedContext = ctx
		err = handler(srv, wrapped)
		return err
	}
}

func LoggingStreamServerInterceptor(entry *log.Entry) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		took := time.Since(start)
		line := entry.WithFields(log.Fields{
			"took":   took,
			"method": info.FullMethod,
		})
		if err != nil {
			st, ok := status.FromError(err)
			if ok {
				line = line.WithFields(log.Fields{
					"error_code": st.Code(),
					"error_desc": st.Message(),
				})
			} else {
				line = line.WithError(err)
			}
			line.Warn("grpc request done with error")
		} else {
			line.Debug("grpc request done")
		}
		return err
	}
}

func AuthenticationUnaryServerInterceptor(authenticator Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		// authenticate first
		ctx, err = authenticator.Authenticate(ctx)
		if err != nil {
			return
		}
		resp, err = handler(ctx, req)
		return
	}
}

func LoggingUnaryServerInterceptor(entry *log.Entry) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		start := time.Now()
		resp, err = handler(ctx, req)
		took := time.Since(start)
		line := entry.WithFields(log.Fields{
			"took":   took,
			"method": info.FullMethod,
		})
		if err != nil {
			st, ok := status.FromError(err)
			if ok {
				line = line.WithFields(log.Fields{
					"error_code": st.Code(),
					"error_desc": st.Message(),
				})
			} else {
				line = line.WithError(err)
			}
			line.Warn("grpc request done with error")
		} else {
			line.Debug("grpc request done")
		}
		return
	}
}
