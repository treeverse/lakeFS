package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

type pathRequest struct {
	path      string
	replyChan chan readResponse
}

type readRequest struct {
	bufKey  bufferingKey
	pathReq pathRequest
}
type readResponse struct {
	entry *Entry
	err   error
}

type readBatch struct {
	startTime time.Time
	pathList  []pathRequest
}

type bufferingKey struct {
	repository string
	ref        Ref
}

type batchReadMessage struct {
	key   bufferingKey
	batch []pathRequest
}

func (c *cataloger) initBatchEntryReader() {
	go c.readOrchestrator()
	c.wg.Add(1)
	for i := 0; i < c.batchParams.ReadersNum; i++ {
		go c.readEntriesBatch()
		c.wg.Add(1)
	}
}

func (c *cataloger) dbBatchEntryRead(repository, path string, ref Ref) (*Entry, error) {
	replyChan := make(chan readResponse, 1) // used for a single return status message.
	// channel written to and closed by readEntriesBatch
	request := &readRequest{
		bufferingKey{repository, ref},
		pathRequest{path, replyChan},
	}
	c.readEntryRequestChan <- request
	select {
	case response := <-replyChan:
		return response.entry, response.err
	case <-time.After(time.Second * time.Duration(c.batchParams.ReadEntryMaxWaitSec)):
		return nil, ErrReadEntryTimeout
	}
}

func (c *cataloger) readOrchestrator() {
	defer c.wg.Done()
	bufferingMap := make(map[bufferingKey]*readBatch)
	timer := time.NewTimer(time.Microsecond * time.Duration(c.batchParams.ScanTimeoutMicroSec))
	for {
		if len(bufferingMap) > 0 {
			timer.Reset(time.Microsecond * time.Duration(c.batchParams.ScanTimeoutMicroSec))
		}
		select {
		case request, moreEntries := <-c.readEntryRequestChan:
			if !moreEntries {
				return // shutdown
			}
			batch, exists := bufferingMap[request.bufKey]
			if !exists {
				batch = &readBatch{
					startTime: time.Now(),
					pathList:  make([]pathRequest, 0, c.batchParams.EntriesReadAtOnce),
				}
				bufferingMap[request.bufKey] = batch
			}
			batch.pathList = append(batch.pathList, request.pathReq)
			if len(batch.pathList) == c.batchParams.EntriesReadAtOnce {
				c.entriesReadBatchChan <- batchReadMessage{request.bufKey, batch.pathList}
				delete(bufferingMap, request.bufKey)
			}
		case <-timer.C:
			for k, v := range bufferingMap {
				if time.Since(v.startTime) > time.Microsecond*time.Duration(c.batchParams.BatchDelayMicroSec) {
					c.entriesReadBatchChan <- batchReadMessage{k, v.pathList}
					delete(bufferingMap, k)
				}
			}
		}
	}
}

func (c *cataloger) readEntriesBatch() {
	defer c.wg.Done()
	for {
		message, more := <-c.entriesReadBatchChan
		if !more {
			return
		}
		ctx := context.Background()
		retInterface, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
			var entList []*Entry
			bufKey := message.key
			pathReqList := message.batch
			branchID, err := c.getBranchIDCache(tx, bufKey.repository, bufKey.ref.Branch)
			if err != nil {
				return entList, err
			}

			lineage, err := getLineage(tx, branchID, bufKey.ref.CommitID)
			if err != nil {
				return entList, fmt.Errorf("get lineage: %w", err)
			}

			p := make([]string, len(pathReqList))
			for i, s := range pathReqList {
				p[i] = s.path
			}
			readExpr := sq.Select("path", "physical_address", "creation_date", "size", "checksum", "metadata", "is_expired").
				FromSelect(sqEntriesLineage(branchID, bufKey.ref.CommitID, lineage), "entries").
				Where(sq.And{sq.Eq{"path": p}, sq.Expr("not is_deleted")})
			sql, args, err := readExpr.PlaceholderFormat(sq.Dollar).ToSql()
			if err != nil {
				return entList, fmt.Errorf("build sql: %w", err)
			}
			err = tx.Select(&entList, sql, args...)
			return entList, err
		}, c.txOpts(ctx, db.ReadOnly(), db.WithIsolationLevel(sql.LevelReadCommitted))...)
		// send  entries to each requestor on the provided one-time channel
		if err != nil {
			c.log.WithError(err).Warn("error reading batch of entries")
		}
		entList := retInterface.([]*Entry)
		entMap := make(map[string]*Entry)
		for _, ent := range entList {
			entMap[ent.Path] = ent
		}
		for _, pathReq := range message.batch {
			e, exists := entMap[pathReq.path]
			if exists {
				pathReq.replyChan <- readResponse{e, nil}
			} else {
				pathReq.replyChan <- readResponse{nil, db.ErrNotFound}
			}
			close(pathReq.replyChan)
		}
	}
}
