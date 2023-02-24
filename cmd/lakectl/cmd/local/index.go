package local

import (
	"fmt"
	"io/fs"
	"math"
	"os"
	"path"
	"time"

	"gopkg.in/yaml.v3"
)

type Diff struct {
	Added    []string
	Removed  []string
	Modified []string
}

func NewDiff() *Diff {
	return &Diff{
		Added:    make([]string, 0),
		Removed:  make([]string, 0),
		Modified: make([]string, 0),
	}
}

func (d *Diff) IsClean() bool {
	return len(d.Added) == 0 && len(d.Modified) == 0 && len(d.Removed) == 0
}

func DiffPath(localDirectory string) (*Diff, error) {
	list := NewDiff()
	index, err := ReadIndex(localDirectory)
	if err != nil {
		return nil, err
	}
	if index == nil {
		return nil, fmt.Errorf("no index file found for directory '%s'", localDirectory)
	}
	visited := make(map[string]bool)
	fileSystem := os.DirFS(localDirectory)
	err = fs.WalkDir(fileSystem, ".", func(p string, d fs.DirEntry, err error) error {
		if p == "." || p == IndexFileName {
			return nil // ignore index and root dir
		}
		fullPath := path.Join(localDirectory, p)
		visited[p] = true
		if d.IsDir() {
			return nil // TODO (ozk): also sync empty directories
		}
		entry := index.Find(p)
		if entry == nil {
			// this file is new!
			list.Added = append(list.Added, p)
			return nil
		}
		// we have an entry, this is either the same or modified ;)
		info, err := os.Stat(fullPath)
		if err != nil {
			return fmt.Errorf("could not stat file %s: %w", fullPath, err)
		}
		if entry.SizeBytes != info.Size() {
			list.Modified = append(list.Modified, p)
			return nil
		}
		if math.Abs(time.Unix(entry.Mtime, 0).Sub(info.ModTime()).Seconds()) > 0 {
			hashCode, err := GetFileSha1(fullPath)
			if err != nil {
				return err
			}
			if entry.Sha1 != hashCode {
				list.Modified = append(list.Modified, p)
			}
			return nil
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// now let's go over anything that might have been deleted
	for _, obj := range index.Objects {
		if _, exists := visited[obj.Path]; !exists {
			list.Removed = append(list.Removed, obj.Path)
		}
	}

	return list, nil
}

func WriteIndex(directory string, index *Index) error {
	data, err := yaml.Marshal(index)
	if err != nil {
		return err
	}
	return os.WriteFile(path.Join(directory, IndexFileName), data, DefaultFileMask)
}

func ReadIndex(directory string) (*Index, error) {
	indexLoc := path.Join(directory, IndexFileName)
	fileExists, err := FileExists(indexLoc)
	if err != nil {
		return nil, err
	}
	if !fileExists {
		return nil, nil
	}
	data, err := os.ReadFile(indexLoc)
	if err != nil {
		return nil, err
	}
	index := &Index{}
	err = yaml.Unmarshal(data, index)
	if err != nil {
		return nil, err
	}
	return index, nil
}

func InitEmptyIndex(localDirectory string) error {
	return WriteIndex(
		localDirectory,
		&Index{
			Objects: (&ObjectTracker{}).GetObjects(),
		},
	)
}
