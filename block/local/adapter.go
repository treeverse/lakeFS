package local

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
	"hash"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
)

type LocalFSAdapter struct {
	path string
	ctx  context.Context
}

func (l *LocalFSAdapter) WithContext(ctx context.Context) block.Adapter {
	return &LocalFSAdapter{
		path: l.path,
		ctx:  ctx,
	}
}

func NewLocalFSAdapter(path string) (block.Adapter, error) {
	stt, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !stt.IsDir() {
		return nil, fmt.Errorf("path provided is not a valid directory")
	}

	//if unix.Access(path, unix.W_OK) != nil {
	if !isDirectoryWritable(path) {
		return nil, fmt.Errorf("path provided is not writable")
	}
	return &LocalFSAdapter{path: path, ctx: context.Background()}, nil
}

func (l *LocalFSAdapter) getPath(identifier string) string {
	return path.Join(l.path, identifier)
}

func (l *LocalFSAdapter) Put(_ string, identifier string, _ int64, reader io.Reader) error {
	path := l.getPath(identifier)
	f, err := os.Create(path)
	defer f.Close()
	_, err = io.Copy(f, reader)
	if err != nil {
		return err
	}
	return nil
}

func (l *LocalFSAdapter) Remove(_ string, identifier string) error {
	path := l.getPath(identifier)
	err := os.Remove(path)
	return err
}

func (l *LocalFSAdapter) Get(_ string, identifier string) (reader io.ReadCloser, err error) {
	path := l.getPath(identifier)
	f, err := os.OpenFile(path, os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (l *LocalFSAdapter) GetRange(_ string, identifier string, start int64, end int64) (io.ReadCloser, error) {
	path := l.getPath(identifier)
	f, err := os.OpenFile(path, os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}
	_, err = f.Seek(start, 0)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (l *LocalFSAdapter) GetAdapterType() string {
	return "local"
}

func isDirectoryWritable(pth string) bool {
	// test ability to write to directory.
	// as there is no simple way to test this in windows, I prefer the "brute force" method
	// of creating s dummy file. will work in any OS.
	// speed is not an issue, as this will be activated very few times during startup

	fileName := path.Join(pth, "dummy.tmp")
	os.Remove(fileName)
	file, err := os.Create(fileName)
	if err == nil {
		file.Close()
		os.Remove(fileName)
		return true
	} else {
		return false
	}
}

func (l *LocalFSAdapter) CreateMultiPartUpload(repo string, identifier string, r *http.Request) (string, error) {
	if strings.Contains(identifier, "/") {
		fullPath := l.getPath(identifier)
		fullDir := path.Dir(fullPath)
		err := os.MkdirAll(fullDir, 0755)
		if err != nil {
			fmt.Errorf("failed to create directory: " + fullDir)
			return "", nil
		}

	}
	x := ([16]byte(uuid.New()))
	uploadId := hex.EncodeToString(x[:])
	uploadId += uploadId
	return uploadId, nil
}

func (l *LocalFSAdapter) UploadPart(repo string, identifier string, sizeBytes int64, reader io.Reader, uploadId string, partNumber int64) (string, error) {
	md5Read := newMd5Reader(reader)
	fName := identifier + "-" + strconv.Itoa(int(partNumber))
	err := l.Put("", fName, -1, md5Read)
	ETag := "\"" + hex.EncodeToString(md5Read.md5.Sum(nil)) + "\""
	return ETag, err
}
func (l *LocalFSAdapter) AbortMultiPartUpload(repo string, identifier string, uploadId string) error {
	return nil
}
func (l *LocalFSAdapter) CompleteMultiPartUpload(repo string, identifier string, uploadId string, XMLmultiPartComplete []byte) (*string, int64, error) {
	var CompleteMultipartStaging struct{ Part []*s3.CompletedPart }
	//uploadId = s.uploadIdTranslator.TranslateUploadId(uploadId)
	err := xml.Unmarshal([]byte(XMLmultiPartComplete), &CompleteMultipartStaging)
	if err != nil {
		fmt.Errorf("failed parcing received XML: " + string(XMLmultiPartComplete))
		return nil, 0, err
	}

}

func (l *LocalFSAdapter) InjectSimulationId(u UploadIdTranslator) {

}

type md5Reader struct {
	md5            hash.Hash
	originalReader io.Reader
	copiedSize     int64
}

func (s *md5Reader) Read(p []byte) (int, error) {
	len, err := s.originalReader.Read(p)
	if len > 0 {
		s.md5.Write(p[0:len])
		s.copiedSize += int64(len)
	}
	return len, err
}

func newMd5Reader(body io.Reader) (s *md5Reader) {
	s = new(md5Reader)
	s.md5 = md5.New()
	s.originalReader = body
	return
}
