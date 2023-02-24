package local

import (
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/go-openapi/swag"
	"gopkg.in/yaml.v3"

	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	ManifestFileName = "data.yaml"
)

var (
	ErrNoManifest            = errors.New("manifest file not found")
	ErrManifestAlreadyExists = errors.New("manifest already exists")
	ErrNoSuchSource          = errors.New("source not defined")
)

type Source struct {
	RemotePath string `yaml:"lakefs_path"`
}

type Manifest struct {
	Remote  string            `yaml:"lakefs_ref"`
	Head    string            `yaml:"current_head,omitempty"`
	Sources map[string]Source `yaml:"sources,omitempty"`

	fileLocation string
}

func (m *Manifest) RemoteURI() (*uri.URI, error) {
	return uri.Parse(m.Remote)
}

func (m *Manifest) RemoteHeadURI() (*uri.URI, error) {
	u, err := uri.Parse(m.Remote)
	if err != nil {
		return nil, err
	}
	return &uri.URI{
		Repository: u.Repository,
		Ref:        m.Head,
	}, nil
}

func (m *Manifest) HasSource(target string) bool {
	_, exists := m.Sources[target]
	return exists
}

func (m *Manifest) SetSource(target, remote string) {
	if m.Sources == nil {
		m.Sources = make(map[string]Source)
	}
	m.Sources[target] = Source{
		RemotePath: remote,
	}
}

func (m *Manifest) GetSource(target string) (*Source, error) {
	src, exists := m.Sources[target]
	if !exists {
		return nil, fmt.Errorf("%s: %w", target, ErrNoSuchSource)
	}
	return &src, nil
}

func (m *Manifest) RemoteURIForPath(p string, latest bool) (*uri.URI, error) {
	remoteUri, err := m.RemoteURI()
	if err != nil {
		return nil, err
	}
	u := &uri.URI{
		Repository: remoteUri.Repository,
		Ref:        remoteUri.Ref,
		Path:       swag.String(p),
	}
	if !latest && m.Head != "" {
		u.Ref = m.Head
	}
	return u, nil
}

func CreateManifest(location string) (*Manifest, error) {
	manifestLocation := path.Join(location, ManifestFileName)
	manifestExists, err := FileExists(manifestLocation)
	if err != nil {
		return nil, err
	}
	if manifestExists {
		return nil, fmt.Errorf("location: '%s': %w", location, ErrManifestAlreadyExists)
	}

	m := &Manifest{}
	m.fileLocation = manifestLocation
	return m, nil
}

func LoadManifest(location string) (*Manifest, error) {
	manifestLocation := path.Join(location, ManifestFileName)
	manifestExists, err := FileExists(manifestLocation)
	if err != nil {
		return nil, err
	}
	if !manifestExists {
		return nil, fmt.Errorf("location: '%s': %w", location, ErrNoManifest)
	}
	data, err := os.ReadFile(manifestLocation)
	if err != nil {
		return nil, err
	}
	m := &Manifest{}
	err = yaml.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	m.fileLocation = manifestLocation
	return m, nil
}

func (m *Manifest) Save() error {
	// ensure directory exists first
	data, err := yaml.Marshal(m)
	if err != nil {
		return err
	}
	return os.WriteFile(m.fileLocation, data, DefaultFileMask)
}

type Locator interface {
	RelativeToRoot(string) (string, error)
}

func IsDataClean(l Locator, manifest *Manifest) (bool, error) {
	for pathInRepository := range manifest.Sources {
		fullPath, err := l.RelativeToRoot(pathInRepository)
		if err != nil {
			return false, err
		}
		dirExists, err := DirectoryExists(fullPath)
		if err != nil {
			return false, err
		}
		if dirExists {
			diffResults, err := DiffPath(fullPath)
			if err != nil {
				return false, err
			}
			if !diffResults.IsClean() {
				return false, nil
			}
		}
	}
	return true, nil
}
