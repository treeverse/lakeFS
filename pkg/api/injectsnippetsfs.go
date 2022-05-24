package api

import (
	"bytes"
	"fmt"
	"html"
	"io"
	"io/fs"
	"time"

	"github.com/treeverse/lakefs/pkg/api/params"
)

type InjectSnippetsFS struct {
	fs.FS
	name    string
	content string
}

func NewInjectIndexFS(fsys fs.FS, name string, marker string, snippets []params.CodeSnippet) (fs.FS, error) {
	if len(snippets) == 0 {
		// no snippets, return the original marker
		return fsys, nil
	}

	// read the content and inject snippets
	f, err := fsys.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	all, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}

	// inject snippets code into content
	codeSnippets := renderCodeSnippets(snippets)
	contentBytes := bytes.ReplaceAll(all, []byte(marker), codeSnippets)
	return &InjectSnippetsFS{
		FS:      fsys,
		name:    name,
		content: string(contentBytes),
	}, nil
}

func renderCodeSnippets(snippets []params.CodeSnippet) []byte {
	var b bytes.Buffer
	for _, item := range snippets {
		_, _ = b.WriteString("<!-- snippet: " + html.EscapeString(item.ID) + " -->")
		_, _ = b.WriteString(item.Code)
	}
	return b.Bytes()
}

func (i *InjectSnippetsFS) Open(name string) (fs.File, error) {
	if name != i.name {
		return i.FS.Open(name)
	}
	return &memFile{
		at:   0,
		Name: name,
		data: []byte(i.content),
	}, nil
}

type memFile struct {
	at   int64
	Name string
	data []byte
}

func (f *memFile) Close() error {
	return nil
}

func (f *memFile) Stat() (fs.FileInfo, error) {
	return &memFileFileInfo{file: f}, nil
}

func (f *memFile) Read(b []byte) (int, error) {
	i := 0
	dataLen := int64(len(f.data))
	for f.at < dataLen && i < len(b) {
		b[i] = f.data[f.at]
		i++
		f.at++
	}
	if f.at >= dataLen {
		return i, io.EOF
	}
	return i, nil
}

type memFileFileInfo struct {
	file *memFile
}

func (s *memFileFileInfo) Name() string       { return s.file.Name }
func (s *memFileFileInfo) Size() int64        { return int64(len(s.file.data)) }
func (s *memFileFileInfo) Mode() fs.FileMode  { return fs.ModeTemporary }
func (s *memFileFileInfo) ModTime() time.Time { return time.Time{} }
func (s *memFileFileInfo) IsDir() bool        { return false }
func (s *memFileFileInfo) Sys() interface{}   { return nil }
