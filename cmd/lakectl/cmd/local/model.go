package local

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	SpecVersion  = 1
	IndexVersion = 1
	SpecFileName = "Runfile.yaml"
	LockFileName = "Runfile.lock.yaml"
)

var (
	ErrUnorderedPaths = errors.New("paths are not sorted lexicographically")
	ErrWrongSize      = errors.New("size read does not match object metadata")
)

type Object struct {
	Path      string `yaml:"poth"`
	Mtime     int64  `yaml:"mtime"`
	SizeBytes int64  `yaml:"size"`
	Sha1      string `yaml:"sha1"`
}

type Index struct {
	Ref     string   `yaml:"ref"`
	Objects []Object `yaml:"objects"`
}

func (index *Index) Find(p string) *Object {
	i := sort.Search(len(index.Objects), func(i int) bool {
		return p <= index.Objects[i].Path
	})
	if i < len(index.Objects) && index.Objects[i].Path == p {
		return &index.Objects[i]
	}
	return nil
}

type ObjectTracker struct {
	m       sync.Mutex
	objects []Object
}

func (t *ObjectTracker) AddExisting(path, sha1 string, sizeBytes, mtime int64) {
	object := Object{
		Path:      path,
		Mtime:     mtime,
		SizeBytes: sizeBytes,
		Sha1:      sha1,
	}
	t.m.Lock()
	defer t.m.Unlock()
	if len(t.objects) > 0 {
		t.objects = append(t.objects, object)
	} else {
		t.objects = []Object{object}
	}
}

func (t *ObjectTracker) Add(path string, mtime, sizeBytes int64, reader io.Reader, writer io.Writer) error {
	h := sha1.New()
	hashingReader := io.TeeReader(reader, h)
	bytesCopied, err := io.Copy(writer, hashingReader)
	if err != nil {
		return err
	}
	if bytesCopied != sizeBytes {
		return ErrWrongSize
	}
	object := Object{
		Path:      path,
		Mtime:     mtime,
		SizeBytes: sizeBytes,
		Sha1:      fmt.Sprintf("%x", h.Sum(nil)),
	}
	t.m.Lock()
	defer t.m.Unlock()
	if len(t.objects) > 0 {
		t.objects = append(t.objects, object)
	} else {
		t.objects = []Object{object}
	}
	return nil
}

func (t *ObjectTracker) GetObjects() []Object {
	slice := t.objects
	sort.SliceStable(slice, func(i, j int) bool {
		return slice[i].Path < slice[j].Path
	})
	return slice
}

type DockerExec struct {
	Image        string   `yaml:"image,omitempty"`
	BuildContext string   `yaml:"build,omitempty"`
	Cmd          []string `yaml:"cmd,omitempty"`
	Args         []string `yaml:"args,omitempty"`
	Environ      []string `yaml:"environ,omitempty"`
}

type MountPoint struct {
	Source string `yaml:"source"`
	Target string `yaml:"target"`
}

type RunSpec struct {
	SpecVersion int          `yaml:"spec_version"`
	Exec        DockerExec   `yaml:"exec"`
	Sources     []MountPoint `yaml:"sources,omitempty"`
}

type Source struct {
	Remote    string `yaml:"remote"`
	AtVersion string `yaml:"at_version,omitempty"`
}

func (s Source) RemoteURI() (*uri.URI, error) {
	return uri.Parse(s.Remote)
}

type SourcesConfig struct {
	Sources map[string]Source `yaml:"sources"`
}
