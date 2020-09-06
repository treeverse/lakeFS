package catalog

import (
	"context"
	"github.com/treeverse/lakefs/db"
	"strconv"
)

const (
	PartNumber              = 10
	OperationsTableNameBase = `catalog_merge_copy_objects_`
)

func (c *cataloger) CreateMergeJob(ctx context.Context, branchID int64, currentCommitID, previousCommitID CommitID) error {
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		var jobID int
		err := tx.Get(&jobID, `INSERT INTO catalog_jobs (task_type,parts_number) values ($1,$2) RETURNING job_id`, "merge_copy", PartNumber)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec(`INSERT INTO catalog_job_parts(job_id,part_id) SELECT $1,generate_series FROM generate_series(1,$2)`, jobID, PartNumber)
		if err != nil {
			return nil, err
		}
		// create job partition table
		jobIDstr := strconv.Itoa(jobID)
		jobTablePartition := OperationsTableNameBase + jobIDstr
		ddl := "create table " + jobTablePartition + " \n" +
			`partition of catalog_merge_copy_objects
				for values in (` + jobIDstr + `)
				partition by list (part_id)`
		_, err = tx.Exec(ddl)
		if err != nil {
			return nil, err
		}
		for i := 1; i <= PartNumber; i++ {
			partNoStr := strconv.Itoa(i)
			PartJobTablePartition := jobTablePartition + "_" + partNoStr
			partDdl := "create table " + PartJobTablePartition +
				"\npartition of " + jobTablePartition +
				"\nfor values in (" + partNoStr + ")"
			_, err = tx.Exec(partDdl)
			if err != nil {
				return nil, err
			}
		}
		// delete instructions are done first to free up S3 space
		// delete commands - pathes that stoped to exist in the current commit, and a new version of them
		// was not created in the current commit
		deleteSql := `INSERT INTO catalog_merge_copy_objects (job_id,part_id,operation,path) 
						SELECT $1,floor(random()*($2)+1),0,path FROM catalog_entries e1
						WHERE branch_id = $3 and max_commit=$4 
						AND not exists (SELECT * FROM catalog_entries e2
								WHERE e1.path = e2.path AND e2.branch_id = $3 AND e2.min_commit = $5)`
		_, err = tx.Exec(deleteSql, jobID, PartNumber, branchID, previousCommitID, currentCommitID)
		if err != nil {
			return nil, err
		}
		copySql := `INSERT INTO catalog_merge_copy_objects (job_id,part_id,operation,path,source_name) 
						SELECT $1,floor(random()*($2)+1),1,path,physical_address FROM catalog_entries 
						WHERE branch_id = $3 and min_commit=$4`
		_, err = tx.Exec(copySql, jobID, PartNumber, branchID, currentCommitID)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}
