package catalog

import (
	"database/sql"
	"fmt"
	"sync"
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

func (c *cataloger) readEntriesBatchOrchestrator() {
	entriesReadBatchChan := make(chan batchReadMessage, 1)
	var readersWG sync.WaitGroup
	defer func() {
		close(entriesReadBatchChan)
		readersWG.Wait()
		c.wg.Done()
	}()

	readersWG.Add(c.BatchRead.Readers)
	for i := 0; i < c.BatchRead.Readers; i++ {
		go c.readEntriesBatch(&readersWG, entriesReadBatchChan)
	}
	bufferingMap := make(map[bufferingKey]*readBatch)
	timer := time.NewTimer(c.BatchRead.ScanTimeout)
	for {
		if len(bufferingMap) > 0 {
			timer.Reset(c.BatchRead.ScanTimeout)
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
					pathList:  make([]pathRequest, 0, c.BatchRead.EntriesAtOnce),
				}
				bufferingMap[request.bufKey] = batch
			}
			batch.pathList = append(batch.pathList, request.pathReq)
			if len(batch.pathList) == c.BatchRead.EntriesAtOnce {
				entriesReadBatchChan <- batchReadMessage{key: request.bufKey, batch: batch.pathList}
				delete(bufferingMap, request.bufKey)
			}
		case <-timer.C:
			for k, v := range bufferingMap {
				if time.Since(v.startTime) > c.BatchRead.Delay {
					entriesReadBatchChan <- batchReadMessage{key: k, batch: v.pathList}
					delete(bufferingMap, k)
				}
			}
		}
	}
}

func (c *cataloger) readEntriesBatch(wg *sync.WaitGroup, inputBatchChan chan batchReadMessage) {
	defer wg.Done()
	for {
		message, more := <-inputBatchChan
		if !more {
			return
		}
		entList, err := c.dbSelectBatchEntries(message.key.repository, message.key.ref, message.batch)
		if err != nil {
			c.log.WithError(err).Warn("error reading batch of entries")
		}
		// send entries to each request on the provided one-time channel
		entMap := make(map[string]*Entry)
		for _, ent := range entList {
			entMap[ent.Path] = ent
		}
		for _, pathReq := range message.batch {
			var response readResponse
			ent, ok := entMap[pathReq.path]
			switch {
			case ok:
				response.entry = ent
			case err != nil:
				response.err = err
			default:
				response.err = ErrEntryNotFound
			}
			pathReq.replyChan <- response
			close(pathReq.replyChan)
		}
	}
}

func (c *cataloger) dbSelectBatchEntries(repository string, ref Ref, pathReqList []pathRequest) ([]*Entry, error) {
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, ref.Branch)
		if err != nil {
			return nil, err
		}
		// prepare list of paths
		p := make([]string, len(pathReqList))
		for i, s := range pathReqList {
			p[i] = s.path
		}
		// prepare query
		readExpr, err := LineageSelect(branchID, p, ref.CommitID, tx, true)
		if err != nil {
			return nil, fmt.Errorf("LineageSelect error : %w", err)
		}
		s := sq.DebugSqlizer(readExpr)
		_ = s
		query, args, err := readExpr.PlaceholderFormat(sq.Dollar).ToSql()
		if err != nil {
			return nil, fmt.Errorf("build sql: %w", err)
		}
		// select entries
		var entries []*Entry
		err = tx.Select(&entries, query, args...)
		if err != nil {
			return nil, fmt.Errorf("select entries: %w", err)
		}
		return entries, nil
	}, db.WithLogger(c.log), db.ReadOnly(), db.WithIsolationLevel(sql.LevelReadCommitted))
	if err != nil {
		return nil, err
	}
	return res.([]*Entry), nil
}
