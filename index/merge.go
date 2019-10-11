package index

import (
	"sort"
	"strings"
	"time"
	"versio-index/ident"
	"versio-index/index/model"
	"versio-index/index/path"
	pth "versio-index/index/path"

	"golang.org/x/xerrors"
)

type changeset struct {
	address string
	entries []*model.Entry
}

func groupChangeSet(entries []*model.WorkspaceEntry) (grouped map[string][]*model.WorkspaceEntry, depthIndex []string) {
	grouped = make(map[string][]*model.WorkspaceEntry)
	for _, entry := range entries {
		p := path.New(entry.GetPath())
		treePath, _ := p.Pop()
		if _, exists := grouped[treePath.String()]; !exists {
			grouped[treePath.String()] = make([]*model.WorkspaceEntry, 0)
		}
		grouped[treePath.String()] = append(grouped[treePath.String()], entry)
	}

	// iterate over groups, starting with the deeper ones and going up
	depthIndex = make([]string, len(grouped))
	i := 0
	for key, _ := range grouped {
		depthIndex[i] = key
		i++
	}
	sort.SliceStable(depthIndex, func(i, j int) bool {
		return len(path.New(depthIndex[i]).SplitParts()) > len(path.New(depthIndex[j]).SplitParts())
	})
	return
}

func partialCommit(tx Transaction, repo *model.Repo, branch string) (string, error) {
	var empty string
	readCache := make(map[string][]*model.Entry)
	changeCache := make(map[string]*changeset)
	// 0. read current branch
	branchData, err := getOrCreateBranch(tx, repo, branch)
	if err != nil {
		return empty, err
	}

	// 1. iterate all changes in the current workspace
	entries, err := tx.ListWorkspace(branch)
	if err != nil {
		return empty, err
	}

	// group by containing tree
	grouped, depthIndex := groupChangeSet(entries)

	// merge with original tree if it exists
	currentAddress := branchData.GetWorkspaceRoot()

	// for each directory, starting with the deepest ones
	var dirEntries []*model.Entry
	for _, currentPath := range depthIndex {
		if dirEntries, exist := readCache[currentPath]; !exist {
			parts := path.New(currentPath).SplitParts()
			for _, part := range parts {
				entry, err := tx.ReadEntry(currentAddress, "d", part)
				if xerrors.Is(err, ErrNotFound) {
					// new directory
					dirEntries = make([]*model.Entry, 0)
					readCache[currentPath] = dirEntries
					currentAddress = empty
					break
				}
				if err != nil {
					return "", err
				}
				currentAddress = entry.GetAddress()
			}
			// for the given path, get list of entries
			dirEntries, err = tx.ListEntries(currentAddress)
			readCache[currentPath] = dirEntries
		}
		if err != nil {
			return empty, err
		}

		// merge lists
		newEntries := grouped[currentPath]

		// iterate sorted lists, merging them
		combinedEntries := make([]*model.Entry, 0)
		oldindex := 0
		newindex := 0
		var nextold *model.Entry
		var nextnew *model.WorkspaceEntry
		for {
			if oldindex >= len(dirEntries) && newindex >= len(newEntries) {
				break // done with both lists
			}

			if oldindex < len(dirEntries) {
				nextold = dirEntries[oldindex]
			} else {
				nextold = nil
			}
			if newindex < len(newEntries) {
				nextnew = newEntries[newindex]
			} else {
				nextnew = nil
			}

			if nextold != nil && nextnew != nil {
				// compare them
				_, nextNewName := pth.New(nextnew.GetPath()).Pop()
				compareResult := strings.Compare(nextold.Name, nextNewName)
				if compareResult == 0 {
					// existing path!
					oldindex++ // we don't need the old value
					newindex++
					if nextnew.GetTombstone() != nil {
						// this is a deletion, simply skip entry
						// TODO: GC
						continue
					} else {
						// this is an overwrite
						combinedEntries = append(combinedEntries, &model.Entry{
							Name:      nextNewName,
							Address:   ident.Hash(nextnew.GetObject()),
							Type:      model.Entry_BLOB,
							Metadata:  nextnew.GetObject().GetMetadata(),
							Timestamp: time.Now().Unix(),
						})
					}
				} else if compareResult == -1 {
					// old is bigger, push new
					if nextnew.GetTombstone() != nil { // makes no sense otherwise
						combinedEntries = append(combinedEntries, &model.Entry{
							Name:      nextNewName,
							Address:   ident.Hash(nextnew.GetObject()),
							Type:      model.Entry_BLOB,
							Metadata:  nextnew.GetObject().GetMetadata(),
							Timestamp: time.Now().Unix(),
						})
					}
					newindex++
				} else {
					// new is bigger, push old
					combinedEntries = append(combinedEntries, nextold)
					oldindex++
				}
			} else if nextold != nil {
				combinedEntries = append(combinedEntries, nextold)
				oldindex++
			} else if nextnew != nil {
				_, nextNewName := pth.New(nextnew.GetPath()).Pop()
				if nextnew.GetTombstone() != nil { // makes no sense otherwise
					combinedEntries = append(combinedEntries, &model.Entry{
						Name:      nextNewName,
						Address:   ident.Hash(nextnew.GetObject()),
						Type:      model.Entry_BLOB,
						Metadata:  nextnew.GetObject().GetMetadata(),
						Timestamp: time.Now().Unix(),
					})
				}
				newindex++
			} else {
				// both are done!!!
				break
			}
		}

		// create a hash for the new entry
		hashes := make([]string, len(combinedEntries))
		for i, ent := range combinedEntries {
			hashes[i] = ident.Hash(ent)
		}

		// current metadata along with list of merged list hashed

		// cache new tree
		changeCache[currentPath] = &changeset{
			address: ident.MultiHash(hashes...),
			entries: combinedEntries,
		}
	}

	// at this point we have a change cache - new trees that reflect workspace changes at their paths
	// update common ancestors until we reach root

	// calc and write all changed trees

	//

	// 2. Apply them to the Merkle root as exists in the branch pointer
	// 3. calculate new Merkle root
	// 4. save it in the branch pointer
	return "", nil
}

func gc(tx Transaction, treeAddress string) {

}
