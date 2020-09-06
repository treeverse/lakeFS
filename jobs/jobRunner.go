package jobs

import (
	"context"
	"database/sql"
	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ScanJobsInterval                 = time.Second * 2
	MaxConcurrentJobs                = 3
	WorkersPerJob                    = 1000
	JobPartLockMajor                 = int32(42)
	OperationCompletionProcessorsNum = 2
)

type endJobMessageType struct {
	job     *JobPartRecord
	success bool
}

type OperationType int

const (
	OperationCopy OperationType = iota
	OperationDelete
)

type JobPartRecord struct {
	Job_id           int32
	Part_id          int32
	Task_type        *string
	Task_description string
}

type mergeObjectResult struct {
	request *mergeObjectRequest
	ok      bool
}

func InitJobActivator(d db.Database) error {
	lockSession, err := d.GetLockSession()
	if err != nil {
		return err
	}
	go jobActivator(d, lockSession)
	return nil
}

func jobActivator(d db.Database, lockSession *sql.Conn) {
	var currentJob int
	endJobChan := make(chan *JobPartRecord)
	activeWorkersCountingLock := NewCountingLock(WorkersPerJob)
	activePartsMap := make(map[int32]bool)
	for {
		select {
		case tm := <-endJobChan:
			part := tm.Part_id
			_, exist := activePartsMap[part]
			if !exist {
				panic("processed part not in active parts map")
				// log does not exist
			} else {
				delete(activePartsMap, part)
				if len(activePartsMap) == 0 {
					currentJob = 0
				}
			}
			released, err := jobPartUnLock(tm, lockSession)
			if err != nil {
				// report error
			}
			if !released {
				//report error
			}
		case <-time.After(ScanJobsInterval):
			if len(activePartsMap) < MaxConcurrentJobs {
				foundPart, partRecord, err := findAndLockJobPart(d, lockSession, currentJob, activePartsMap)
				if err != nil {
					panic(err)
					continue
				}
				if !foundPart {
					continue
				}
				if currentJob == 0 {
					currentJob = int(partRecord.Job_id)
				}
				activePartsMap[partRecord.Part_id] = true
				jobTableName := catalog.OperationsTableNameBase + strconv.Itoa(int(partRecord.Job_id))
				partTableName := jobTableName + "_" + strconv.Itoa(int(partRecord.Part_id))
				go jobPartOrcestrator(d, partRecord, endJobChan, partTableName, jobTableName, activeWorkersCountingLock)
			}
		}
	}
}

func findAndLockJobPart(d db.Database, lockSession *sql.Conn, currentJob int, activePartsMap map[int32]bool) (bool, *JobPartRecord, error) {
	var activeParts []int32
	var selectedJobPart *JobPartRecord
	var jobRecords []*JobPartRecord
	for part, _ := range activePartsMap {
		activeParts = append(activeParts, part)
	}
	getJobsList := sq.Select("j.job_id", "p.part_id", "j.task_type", "j.task_description::text").From("catalog_jobs j").
		Join("catalog_job_parts p using (job_id)")
	// The system can process parts only from a single job. So if it has current parts running - it will not start another job
	// if there are no jobs running - take the lowest(oldest) job number
	if currentJob == 0 {
		getJobsList = getJobsList.
			Where("j.job_id = (SELECT MIN(job_id) FROM catalog_job_parts WHERE NOT completed) AND NOT p.completed")
	} else {
		getJobsList = getJobsList.Where("j.job_id = ?", currentJob).
			Where(sq.NotEq{"part_id": activeParts}).Where("not p.completed")
	}
	sql, args, err := getJobsList.PlaceholderFormat(sq.Dollar).ToSql()
	if err != nil {
		panic(err)
	}
	r, err := d.Transact(func(tx db.Tx) (interface{}, error) {
		// the table is locked to avoid race condition between updating the job status and releasing the part session lock
		_, err := tx.Exec("LOCK TABLE catalog_job_parts in SHARE ROW EXCLUSIVE MODE ")
		if err != nil {
			panic(err)
		}
		err = tx.Select(&jobRecords, sql, args...)
		if err != nil {
			panic(err)
		}
		for _, r := range jobRecords {
			success, err := tryJobPartLock(r, lockSession)
			if err != nil {
				panic(err)
			}
			if success {
				selectedJobPart = r
				return true, nil
			}
		}
		return false, nil
	}, db.ReadOnly())
	found := r.(bool)
	return found, selectedJobPart, nil
}

func tryJobPartLock(r *JobPartRecord, lockSession *sql.Conn) (bool, error) {
	var lockTaken bool
	row := lockSession.QueryRowContext(context.Background(), "select pg_try_advisory_lock($1,$2)", JobPartLockMajor,
		jobPartLockID(r.Job_id, r.Part_id))
	err := row.Scan(&lockTaken)
	if err != nil {
		return false, err
	}
	return lockTaken, nil
}

func jobPartUnLock(r *JobPartRecord, lockSession *sql.Conn) (bool, error) {
	var lockReleased bool
	row := lockSession.QueryRowContext(context.Background(), "select pg_advisory_unlock($1,$2)", JobPartLockMajor,
		jobPartLockID(r.Job_id, r.Part_id))
	err := row.Scan(&lockReleased)
	if err != nil {
		return false, err
	}
	return lockReleased, nil
}

func jobPartLockID(jobID, partID int32) int32 {
	//Postgress advisory lock is defined by two 32 bit integers
	// first integer used to denote this is a job part lock (constant JobPartLockMajor)
	// So job and part id need to squeeze into 31 bits.(to avoid playing with unsigned)
	// use the most significant 10 bits for the job id modulu (assuming there are less than 1024 jobs acticv concurrently)
	// last 21 bits used for part number - up to 2,097,151
	return int32(partID%(1<<21) + (jobID%(1<<10))<<21)
}

type mergeObjectRequest struct {
	Operation   OperationType
	Path        string
	Source_name string
	Ctid        string
	Retry_count int16
}

type mergeOperationResult struct {
	err        error
	errMessage string
	request    *mergeObjectRequest
}

const (
	MaxRowsToDelete    = 1024
	OrcestratorRetries = 3
)

func jobPartOrcestrator(d db.Database, job *JobPartRecord, endJobChan chan *JobPartRecord, partTableName, jobTableName string,
	ActiveWorkersCountingLock *CountingLock) {
	var failureCounter int64
	var orcestratorWg sync.WaitGroup
	jobID := job.Job_id
	partID := job.Part_id
	resultChan := make(chan *mergeOperationResult, WorkersPerJob)
	endOperationsStreamer := make(chan bool)

	for i := 0; i < OrcestratorRetries; i++ {
		orcestratorWg.Add(1 + OperationCompletionProcessorsNum)
		go operationsStreamer(d, jobID, partID, &orcestratorWg, resultChan, endOperationsStreamer, partTableName, ActiveWorkersCountingLock)
		for j := 0; j < OperationCompletionProcessorsNum; j++ {
			go OperationsCompletionProcessor(d, jobID, partID, resultChan, partTableName, &orcestratorWg, &failureCounter)
		}
		orcestratorWg.Wait()
		if failureCounter == 0 {
			break
		}
	}
	terminateJobPart(d, partTableName, jobTableName, jobID, partID, failureCounter == 0)
	endJobChan <- job
}

func operationsStreamer(d db.Database, jobID, partID int32, orcestratorWG *sync.WaitGroup, resultChan chan *mergeOperationResult,
	endOperationsStreamer chan bool, partTableName string, ActiveWorkersCountingLock *CountingLock) {
	var copiersWG sync.WaitGroup
	OperationsChan := make(chan *mergeObjectRequest, WorkersPerJob)
	copiersWG.Add(WorkersPerJob)
	for i := 0; i < WorkersPerJob; i++ {
		go copier(OperationsChan, resultChan, &copiersWG, ActiveWorkersCountingLock.NewCountingLockRef())
	}
	_, _ = d.Transact(func(tx db.Tx) (interface{}, error) {
		rows, err := d.Queryx("SELECT operation,path,source_name,ctid FROM "+partTableName+" WHERE job_id = $1 AND part_id = $2", jobID, partID)
		if err != nil {
			return false, err
		}
		for rows.Next() {
			operation := new(mergeObjectRequest)
			err := rows.StructScan(operation)
			if err != nil {
				//todo
				panic("in scan")
			}
			OperationsChan <- operation
		}
		return nil, err
	}, db.ReadOnly())
	close(OperationsChan) // signals to workers to exit
	copiersWG.Wait()
	close(resultChan) // after all workers exit, there will be no new results
	orcestratorWG.Done()
}

func terminateJobPart(d db.Database, partTableName, jobTableName string, jobID, partID int32, allDone bool) {
	updatePartExpr := sq.Update("catalog_job_parts").
		Where("job_id=? and part_id=?", jobID, partID).
		Set("completed", true).
		Set("succeeded", allDone)
	updateSql, args, err := updatePartExpr.PlaceholderFormat(sq.Dollar).ToSql()
	jobDropSql := "drop table " + jobTableName
	partDropSql := "drop table " + partTableName
	if err != nil {
		panic("error in ToSql")
	}
	_, err = d.Transact(func(tx db.Tx) (interface{}, error) {
		// job parts table is read to see if all parts finished. for this reason it has to be exclusively locked
		// isolation has to be read committed. if we keep the default serializabel isolation
		//there may be a ugly race condition where:
		// a and b finish processing job parts together.
		// 1. a gets the table lock, and updates the job part
		// 2. b is locked on the table
		// 3. a commit
		// 4. b is released from the table lock
		// 5. b updates
		// 6. b reads the jobs to determin if all jobs terminated. because of serializabe isolation mode
		//    b does not see the row a updated
		// 7. The job will never be marked as terminated
		_, err := tx.Exec("LOCK TABLE catalog_job_parts in SHARE ROW EXCLUSIVE MODE ")
		if err != nil {
			panic(err)
		}
		r, err := tx.Exec(updateSql, args...)
		_ = r
		if err != nil {
			panic("error in update")
			return nil, err
		}
		r, err = tx.Exec(partDropSql)
		_ = r
		if err != nil {
			panic("error in drop part partition")
			return nil, err
		}
		var unFinishedParts int
		err = tx.Get(&unFinishedParts,
			"SELECT COUNT(*) FROM catalog_job_parts WHERE job_id = $1  AND completed = FALSE", jobID)
		if err != nil {
			panic(err)
		}
		if unFinishedParts == 0 {
			r, err = tx.Exec("UPDATE catalog_jobs SET completed = TRUE WHERE job_id = $1", jobID)
			r, err = tx.Exec(jobDropSql)
			_ = r
			if err != nil {
				panic("error in drop job partition")
				return nil, err
			}
		}
		return nil, err
	}, db.WithIsolationLevel(sql.LevelReadCommitted))

	if err != nil {
		panic(err)
	}
	return
}

func OperationsCompletionProcessor(d db.Database, jobID, partID int32, resultChan chan *mergeOperationResult, partTableName string,
	orcestratorWG *sync.WaitGroup, failureCounter *int64) {
	deleteCtidList := make([]string, 0, MaxRowsToDelete)
	baseDeleteExpr := sq.Delete(partTableName).Where("job_id = ? and part_id = ?", jobID, partID)
	for operationResult := range resultChan {
		if operationResult.err != nil {
			updateFailedOperation(d, 1, operationResult.err.Error(), partTableName, operationResult.request.Ctid)
			atomic.AddInt64(failureCounter, 1)
		} else {
			deleteCtidList = append(deleteCtidList, operationResult.request.Ctid)
			if len(deleteCtidList) == MaxRowsToDelete {
				deleteOperations(d, baseDeleteExpr, deleteCtidList)
				deleteCtidList = deleteCtidList[:0]
			}
		}
	}
	if len(deleteCtidList) > 0 {
		deleteOperations(d, baseDeleteExpr, deleteCtidList)
	}
	orcestratorWG.Done()
}

func updateFailedOperation(d db.Database, failStatus int, failText, partTableName, ctid string) error {
	updateSql := "UPDATE " + partTableName + ` SET retry_count = retry_count + 1,
												fail_status = $1,fail_text = $2
												WHERE ctid = $3`
	_, err := d.Transact(func(tx db.Tx) (interface{}, error) {
		r, err := d.Exec(updateSql, failStatus, failText, ctid)
		_ = r
		if err != nil {
			panic("error in update")
			return nil, err
		}
		return nil, nil
	}, db.WithIsolationLevel(sql.LevelReadCommitted))
	return err
}

func deleteOperations(d db.Database, baseDeleteExpr sq.DeleteBuilder, deleteCtidList []string) error {
	deleteExpr := baseDeleteExpr.Where(sq.Eq{"ctid": deleteCtidList})
	sqlDelete, args, err := deleteExpr.PlaceholderFormat(sq.Dollar).ToSql()
	if err != nil {
		panic("error in delete operations in ToSql")
		return err
	}
	_, err = d.Transact(func(tx db.Tx) (interface{}, error) {
		r, err := d.Exec(sqlDelete, args...)
		_ = r
		if err != nil {
			panic("error in delete")
			return nil, err
		}
		return nil, err
	}, db.WithIsolationLevel(sql.LevelReadCommitted))
	return err
}

// a dummy implementation
func copier(copyOperationChan chan *mergeObjectRequest, resultChan chan *mergeOperationResult, wg *sync.WaitGroup,
	ActiveWorkersRef *CountingLockRef) {
	defer ActiveWorkersRef.Release(true)
	defer wg.Done()
	for operation := range copyOperationChan {
		ActiveWorkersRef.GetAccess()
		resultChan <- &mergeOperationResult{request: operation}
		time.Sleep(time.Millisecond * 10)
		ActiveWorkersRef.Release(false)
	}
}
