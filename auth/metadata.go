package auth

import (
	"context"
	"math/rand"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

func UpdateMetadataValues(authService Service) error {
	metadata := make(map[string]string)
	metadata["lakefs_version"] = config.Version
	metadata["golang_version"] = runtime.Version()
	metadata["architecture"] = runtime.GOARCH
	metadata["os"] = runtime.GOOS

	// read all the DB values, if applicable
	type HasDatabase interface {
		DB() db.Database
	}
	if d, ok := authService.(HasDatabase); ok {
		conn := d.DB()
		dbMeta, err := conn.Metadata()
		if err == nil {
			for k, v := range dbMeta {
				metadata[k] = v
			}
		}
	}

	// write everything.
	for k, v := range metadata {
		err := authService.SetAccountMetadataKey(k, v)
		if err != nil {
			return err
		}
	}

	return nil

}

func WriteInitialMetadata(authService Service) error {

	err := authService.SetAccountMetadataKey("setup_time", time.Now().Format(time.RFC3339))
	if err != nil {
		return err
	}

	err = authService.SetAccountMetadataKey("installation_id", uuid.Must(uuid.NewUUID()).String())
	if err != nil {
		return err
	}

	return UpdateMetadataValues(authService)
}

type MetadataRefresher struct {
	splay       time.Duration
	interval    time.Duration
	authService Service
	stop        chan bool
	done        chan bool
}

func NewMetadataRefresher(splay, interval time.Duration, authService Service) *MetadataRefresher {
	return &MetadataRefresher{
		splay:       splay,
		interval:    interval,
		authService: authService,
		stop:        make(chan bool),
		done:        make(chan bool),
	}
}

func (m *MetadataRefresher) Start() {
	go func() {
		// sleep random 0-splay
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		splayRandTime := rand.Intn(r.Intn(int(m.splay)))
		splayRandDuration := time.Duration(splayRandTime)
		log := logging.Default().WithFields(logging.Fields{
			"splay":    splayRandDuration.String(),
			"interval": m.interval,
		})
		log.Trace("starting metadata refresher")
		select {
		case <-m.stop:
			m.done <- true
			return
		case <-time.After(splayRandDuration):
			m.update()
		}
		time.Sleep(splayRandDuration)
		m.update()
		stillRunning := true
		for stillRunning {
			select {
			case <-m.stop:
				stillRunning = false
				break
			case <-time.After(m.interval):
				m.update()
			}
		}
		m.done <- true
	}()
}

func (m *MetadataRefresher) update() {
	err := UpdateMetadataValues(m.authService)
	if err != nil {
		logging.Default().WithError(err).Debug("failed refreshing local metadata values")
		return
	}
	logging.Default().Trace("local metadata refreshed")
}

func (m *MetadataRefresher) Shutdown(ctx context.Context) error {
	go func() { m.stop <- true }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.done:
		return nil
	}
}
