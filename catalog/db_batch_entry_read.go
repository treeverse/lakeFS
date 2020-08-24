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

func (c *cataloger) dbBatchEntryRead(repository, path string, ref Ref) (*Entry, error) {
	replyChan := make(chan readResponse, 1) // used for a single return status message.
	// channel written to and closed by readEntriesBatch
	request := &readRequest{
		bufKey: bufferingKey{
			repository: repository,
			ref:        ref,
		},
		pathReq: pathRequest{
			path:      path,
			replyChan: replyChan,
		},
	}
	c.readEntryRequestChan <- request
	select {
	case response := <-replyChan:
		return response.entry, response.err
	case <-time.After(c.batchParams.ReadEntryMaxWait):
		return nil, ErrReadEntryTimeout
	}
}

func (c *cataloger) readOrchestrator() {
	entriesReadBatchChan := make(chan batchReadMessage, 1)
	var readersWG sync.WaitGroup
	defer func() {
		close(entriesReadBatchChan)
		readersWG.Wait()
		c.wg.Done()
	}()

	readersWG.Add(c.batchParams.Readers)
	for i := 0; i < c.batchParams.Readers; i++ {
		go c.readEntriesBatch(&readersWG, entriesReadBatchChan)
	}
	bufferingMap := make(map[bufferingKey]*readBatch)
	timer := time.NewTimer(c.batchParams.ScanTimeout)
	for {
		if len(bufferingMap) > 0 {
			timer.Reset(c.batchParams.ScanTimeout)
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
				entriesReadBatchChan <- batchReadMessage{key: request.bufKey, batch: batch.pathList}
				delete(bufferingMap, request.bufKey)
			}
		case <-timer.C:
			for k, v := range bufferingMap {
				if time.Since(v.startTime) > c.batchParams.BatchDelay {
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
		retInterface, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
			bufKey := message.key
			pathReqList := message.batch
			branchID, err := c.getBranchIDCache(tx, bufKey.repository, bufKey.ref.Branch)
			if err != nil {
				return nil, err
			}

			lineage, err := getLineage(tx, branchID, bufKey.ref.CommitID)
			if err != nil {
				return nil, fmt.Errorf("get lineage: %w", err)
			}

			p := make([]string, len(pathReqList))
			for i, s := range pathReqList {
				p[i] = s.path
			}
			readExpr := sq.Select("path", "physical_address", "creation_date", "size", "checksum", "metadata", "is_expired").
				FromSelect(sqEntriesLineage(branchID, bufKey.ref.CommitID, lineage), "entries").
				Where(sq.And{sq.Eq{"path": p}, sq.Expr("not is_deleted")})
			query, args, err := readExpr.PlaceholderFormat(sq.Dollar).ToSql()
			if err != nil {
				return nil, fmt.Errorf("build sql: %w", err)
			}
			var entList []*Entry
			err = tx.Select(&entList, query, args...)
			if err != nil {
				return nil, fmt.Errorf("select entries: %w", err)
			}
			return entList, nil
		}, db.WithLogger(c.log), db.ReadOnly(), db.WithIsolationLevel(sql.LevelReadCommitted))
		// send entries to each requestor on the provided one-time channel
		var entList []*Entry
		if err != nil {
			c.log.WithError(err).Warn("error reading batch of entries")
		} else {
			entList = retInterface.([]*Entry)
		}
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
