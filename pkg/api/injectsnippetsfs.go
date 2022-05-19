package api

import (
	"bytes"
	"fmt"
	"html"
	"io"
	"io/fs"
	"time"
)

type InjectSnippetsFS struct {
	fs.FS
	name    string
	content string
}

func NewInjectIndexFS(fsys fs.FS, name string, snippets map[string]string) (fs.FS, error) {
	// no snippets, we serve using the name fs
	if len(snippets) == 0 {
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
	bodyEndElement := []byte("</body>")
	idx := bytes.LastIndex(all, bodyEndElement)
	if idx == -1 {
		// TODO(barak): consider error in case there is no body to inject snippet
		return fsys, nil
	}
	var b bytes.Buffer
	_, _ = b.Write(all[0:idx])
	_, _ = b.Write(codeSnippets)
	_, _ = b.Write(all[idx:])
	return &InjectSnippetsFS{
		FS:      fsys,
		name:    name,
		content: b.String(),
	}, nil
}

func renderCodeSnippets(snippets map[string]string) []byte {
	var b bytes.Buffer
	for k, v := range snippets {
		_, _ = b.WriteString("<!-- snippet begin: " + html.EscapeString(k) + " -->\n")
		_, _ = b.WriteString(v)
		_, _ = b.WriteString("<!-- snippet end: " + html.EscapeString(k) + " -->\n")
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
	for f.at < int64(len(f.data)) && i < len(b) {
		b[i] = f.data[f.at]
		i++
		f.at++
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
