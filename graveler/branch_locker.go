package graveler

import (
	"fmt"
	"sync"
)

// branchLocker enforces the branch locking logic
// The logic is as follows:
// allow concurrent writers to branch
// metadata update commands will wait until all current running writers are done
// while metadata update is running or waiting to run, writes and metadata update commands will be blocked
type branchLocker struct {
	locker   sync.Locker
	c        *sync.Cond
	branches map[string]*branchLockerData
}

type branchLockerData struct {
	writers        int32
	metadataUpdate bool
}

func NewBranchLocker() branchLocker {
	m := sync.Mutex{}
	return branchLocker{
		locker:   &m,
		c:        sync.NewCond(&m),
		branches: make(map[string]*branchLockerData),
	}
}

// AquireWrite returns a cancel function to release if write is currently available
// returns ErrBranchUpdateInProgress if metadata update is currently in progress
func (l *branchLocker) AquireWrite(repositoryID RepositoryID, branchID BranchID) (func(), error) {
	key := formatBranchLockerKey(repositoryID, branchID)
	l.locker.Lock()
	defer l.locker.Unlock()
	data := l.branches[key]
	if data == nil {
		data = &branchLockerData{}
		l.branches[key] = data
	} else if data.metadataUpdate {
		return nil, ErrBranchLocked
	}
	data.writers++
	return func() { l.releaseWrite(key) }, nil
}

func (l *branchLocker) releaseWrite(key string) {
	l.locker.Lock()
	defer l.locker.Unlock()
	branchData := l.branches[key]
	branchData.writers--
	if branchData.writers > 0 {
		return
	}
	if branchData.metadataUpdate {
		l.c.Broadcast()
	} else {
		delete(l.branches, key)
	}
}

// AquireMetadataUpdate returns a cancel function to release if metadata update is currently available
// Will wait until all current writers are done
// returns ErrBranchUpdateInProgress if metadata update is currently in progress
func (l *branchLocker) AquireMetadataUpdate(repositoryID RepositoryID, branchID BranchID) (func(), error) {
	key := formatBranchLockerKey(repositoryID, branchID)
	l.locker.Lock()
	defer l.locker.Unlock()
	data := l.branches[key]
	if data == nil {
		data = &branchLockerData{}
		l.branches[key] = data
	} else if data.metadataUpdate {
		// allow just one metadata update at a time
		return nil, ErrBranchLocked
	}
	// wait until all writers will leave
	data.metadataUpdate = true
	for data.writers > 0 {
		l.c.Wait()
	}
	return func() { l.releaseMetadataUpdate(key) }, nil
}

func (l *branchLocker) releaseMetadataUpdate(key string) {
	l.locker.Lock()
	defer l.locker.Unlock()
	delete(l.branches, key)
}

func formatBranchLockerKey(repositoryID RepositoryID, branchID BranchID) string {
	return fmt.Sprintf("%s/%s", repositoryID, branchID)
}
