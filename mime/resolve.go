package mime

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"strings"
)

const (
	Seperator       = "."
	DefaultMimetype = "application/octet-stream"
)

type Resolver struct {
	types map[string]string
}

type Entry struct {
	Source       string   `json:"source"`
	Compressible bool     `json:"compressible"`
	Extensions   []string `json:"extensions,omitempty"`
}

func BuildResolver(jsonDB io.Reader) (*Resolver, error) {
	bytes, err := ioutil.ReadAll(jsonDB)
	if err != nil {
		return nil, err
	}
	data := make(map[string]*Entry)
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		return nil, err
	}

	types := make(map[string]string)
	for extension, entry := range data {
		for _, ext := range entry.Extensions {
			if _, exists := types[ext]; exists {
				current := types[ext]
				// if one of them is the default, prefer the other
				if strings.EqualFold(current, DefaultMimetype) {
					types[ext] = extension
					continue
				} else if strings.EqualFold(extension, DefaultMimetype) {
					continue
				}
				// if none is the default, prefer the lexicographical first, just to make things consistent
				comparison := strings.Compare(current, extension)
				if comparison < 0 {
					continue
				} else {
					types[ext] = extension
					continue
				}
			}
			types[ext] = extension
		}
	}

	return &Resolver{types: types}, nil
}

func (r *Resolver) Resolve(filename string) string {
	// get the extension
	lastSep := strings.LastIndex(filename, Seperator)
	if lastSep == -1 {
		return DefaultMimetype
	}
	if lastSep+1 > len(filename)-1 {
		return DefaultMimetype
	}
	suffix := filename[lastSep+1:]
	mime, exists := r.types[suffix]
	if !exists {
		return DefaultMimetype
	}
	return mime
}
