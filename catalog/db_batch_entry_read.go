package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

const (
	MaxReadQueue         = 10
	ReadTimeout          = time.Second * 600
	ScanTimeout          = time.Microsecond * 500
	waitTimeout          = time.Microsecond * 1000
	MaxEnteriesInRequest = 128
	ReadersNum           = 8
	ReadsPerConnection   = 500
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

var readChan chan *readRequest

func initBatchEntryRead(c *cataloger) {
	readChan = make(chan *readRequest, MaxReadQueue)
	go readOrchestrator(c)
}

func dbBatchEntryRead(repository, path string, ref Ref) (*Entry, error) {
	replyChan := make(chan readResponse, 1)
	defer close(replyChan)
	request := &readRequest{
		bufferingKey{repository, ref},
		pathRequest{path, replyChan},
	}
	readChan <- request
	select {
	case response := <-replyChan:
		return response.entry, response.err
	case <-time.After(ReadTimeout):
		return nil, ErrTimeout
	}
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

func newReadBatch() *readBatch {
	return &readBatch{
		time.Now(),
		make([]pathRequest, 0, MaxEnteriesInRequest),
	}
}

func readOrchestrator(c *cataloger) {
	batchChan := make(chan batchReadMessage, 2)
	for i := 0; i < ReadersNum; i++ {
		go readEntriesBatch(c, batchChan)
	}
	var moreEntries bool
	bufferingMap := make(map[bufferingKey]*readBatch)
	var request *readRequest
	for true {
		request = nil
		if len(bufferingMap) > 0 {
			select {
			case request, moreEntries = <-readChan:
			case <-time.After(ScanTimeout):
			}
		} else {
			request, moreEntries = <-readChan // if there are no pending requests - no need for timeout
		}
		if !moreEntries {
			return
		}
		var batch *readBatch
		var exists bool
		if request != nil {
			batch, exists = bufferingMap[request.bufKey]
			if !exists {
				batch = newReadBatch()
				bufferingMap[request.bufKey] = batch
			}
			batch.pathList = append(batch.pathList, request.pathReq)
		}
		for k, v := range bufferingMap {
			if len(v.pathList) == MaxEnteriesInRequest || time.Now().Sub(v.startTime) > waitTimeout {
				batchChan <- batchReadMessage{k, v.pathList}
				//readEntriesBatch(c, k, v.pathList)
				delete(bufferingMap, k)
			}
		}
	}
}

//func readEntriesBatch(c *cataloger, bufKey bufferingKey, pathReqList []pathRequest) {
//	//ctx := context.Background()
//	branchID, err := c.getBranchIDCache(c.db, bufKey.repository, bufKey.ref.Branch)
//	if err != nil {
//		fmt.Printf("in readEntriesBatch- getBranchIDcache: %w", err)
//		return
//	}
//
//	lineage, err := getLineage(c.db, branchID, bufKey.ref.CommitID)
//	if err != nil {
//		fmt.Printf("in readEntriesBatch- getLineage: %w", err)
//		return
//	}
//
//	//var lineage = []lineageCommit{{3, 14}, {2, 6}, {1, 5}}
//
//	p := make([]string, len(pathReqList))
//	for i, s := range pathReqList {
//		p[i] = s.path
//	}
//	pathInExper := "('" + strings.Join(p, "','") + "')"
//	inExper := sq.Select("path", "physical_address", "creation_date", "size", "checksum", "metadata", "is_expired").
//		FromSelect(sqEntriesLineage(branchID, bufKey.ref.CommitID, lineage), "entries").
//		Where("path in " + pathInExper + " and not is_deleted")
//	deb := sq.DebugSqlizer(inExper)
//	_ = deb
//	sql, args, err := inExper.PlaceholderFormat(sq.Dollar).ToSql()
//	if err != nil {
//		fmt.Printf("in readEntriesBatch- ToSql: %w", err)
//		return
//	}
//
//	var entList []*Entry
//	if err := c.db.Select(&entList, sql, args...); err != nil {
//		fmt.Printf("in readEntriesBatch- Select: %w", err)
//		return
//	}
//	entMap := make(map[string]*Entry)
//	for _, ent := range entList {
//		entMap[ent.Path] = ent
//	}
//	for _, pathReq := range pathReqList {
//		e, exists := entMap[pathReq.path]
//		if exists {
//			pathReq.replyChan <- readResponse{e, nil}
//		} else {
//			pathReq.replyChan <- readResponse{nil, db.ErrNotFound}
//		}
//
//	}
//}

func readEntriesBatch(c *cataloger, batchRead chan batchReadMessage) {
	ctx := context.Background()
	for {
		_, _ = c.db.Transact(func(tx db.Tx) (interface{}, error) {
			for j := 0; j < ReadsPerConnection; j++ {
				message := <-batchRead
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

				//var lineage = []lineageCommit{{3, 14}, {2, 6}, {1, 5}}

				p := make([]string, len(pathReqList))
				for i, s := range pathReqList {
					p[i] = s.path
				}
				pathInExper := "('" + strings.Join(p, "','") + "')"
				inExper := sq.Select("path", "physical_address", "creation_date", "size", "checksum", "metadata", "is_expired").
					FromSelect(sqEntriesLineage(branchID, bufKey.ref.CommitID, lineage), "entries").
					Where("path in " + pathInExper + " and not is_deleted")
				deb := sq.DebugSqlizer(inExper)
				_ = deb
				sql, args, err := inExper.PlaceholderFormat(sq.Dollar).ToSql()
				if err != nil {
					return nil, fmt.Errorf("build sql: %w", err)
				}

				var entList []*Entry
				if err := tx.Select(&entList, sql, args...); err != nil {
					return nil, err
				}
				entMap := make(map[string]*Entry)
				for _, ent := range entList {
					entMap[ent.Path] = ent
				}
				for _, pathReq := range pathReqList {
					e, exists := entMap[pathReq.path]
					if exists {
						pathReq.replyChan <- readResponse{e, nil}
					} else {
						fmt.Print("NOT FOUND " + pathReq.path + " \n")
						pathReq.replyChan <- readResponse{nil, db.ErrNotFound}
					}

				}
			}
			return nil, nil
		}, c.txOpts(ctx, db.ReadOnly(), db.WithIsolationLevel(sql.LevelReadCommitted))...)
		fmt.Print("finished 500 reads in loop\n")
	}
}

//func readEntriesBatch(c *cataloger, bufKey bufferingKey, pathReqList []pathRequest) {
//	ctx := context.Background()
//	_, _ = c.db.Transact(func(tx db.Tx) (interface{}, error) {
//
//		branchID, err := c.getBranchIDCache(tx, bufKey.repository, bufKey.ref.Branch)
//		if err != nil {
//			return nil, err
//		}
//
//		lineage, err := getLineage(tx, branchID, bufKey.ref.CommitID)
//		if err != nil {
//			return nil, fmt.Errorf("get lineage: %w", err)
//		}
//
//		//var lineage = []lineageCommit{{3, 14}, {2, 6}, {1, 5}}
//
//		p := make([]string, len(pathReqList))
//		for i, s := range pathReqList {
//			p[i] = s.path
//		}
//		pathInExper := "('" + strings.Join(p, "','") + "')"
//		inExper := sq.Select("path", "physical_address", "creation_date", "size", "checksum", "metadata", "is_expired").
//			FromSelect(sqEntriesLineage(branchID, bufKey.ref.CommitID, lineage), "entries").
//			Where("path in " + pathInExper + " and not is_deleted")
//		deb := sq.DebugSqlizer(inExper)
//		_ = deb
//		sql, args, err := inExper.PlaceholderFormat(sq.Dollar).ToSql()
//		if err != nil {
//			return nil, fmt.Errorf("build sql: %w", err)
//		}
//
//		var entList []*Entry
//		if err := tx.Select(&entList, sql, args...); err != nil {
//			return nil, err
//		}
//		entMap := make(map[string]*Entry)
//		for _, ent := range entList {
//			entMap[ent.Path] = ent
//		}
//		for _, pathReq := range pathReqList {
//			e, exists := entMap[pathReq.path]
//			if exists {
//				pathReq.replyChan <- readResponse{e, nil}
//			} else {
//				pathReq.replyChan <- readResponse{nil, db.ErrNotFound}
//			}
//
//		}
//		return nil, nil
//	}, c.txOpts(ctx, db.ReadOnly(), db.WithIsolationLevel(sql.LevelReadCommitted))...)
//}
