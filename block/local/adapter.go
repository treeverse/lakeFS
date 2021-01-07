package local

import (
	"context"
	"crypto/md5" //nolint:gosec
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/logging"
)

const BlockstoreType = "local"

type Adapter struct {
	path               string
	ctx                context.Context
	uploadIDTranslator block.UploadIDTranslator
}

var (
	ErrPathNotValid          = errors.New("path provided is not a valid directory")
	ErrPathNotWritable       = errors.New("path provided is not writable")
	ErrInventoryNotSupported = errors.New("inventory feature not implemented for local storage adapter")
	ErrInvalidUploadIDFormat = errors.New("invalid upload id format")
)

func (l *Adapter) WithContext(ctx context.Context) block.Adapter {
	return &Adapter{
		path:               l.path,
		ctx:                ctx,
		uploadIDTranslator: l.uploadIDTranslator,
	}
}

func WithTranslator(t block.UploadIDTranslator) func(a *Adapter) {
	return func(a *Adapter) {
		a.uploadIDTranslator = t
	}
}

func NewAdapter(path string, opts ...func(a *Adapter)) (*Adapter, error) {
	path = filepath.Clean(path)
	stt, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !stt.IsDir() {
		return nil, ErrPathNotValid
	}
	if !isDirectoryWritable(path) {
		return nil, ErrPathNotWritable
	}
	adapter := &Adapter{
		path:               path,
		ctx:                context.Background(),
		uploadIDTranslator: &block.NoOpTranslator{},
	}
	for _, opt := range opts {
		opt(adapter)
	}
	return adapter, nil
}
func resolveNamespace(obj block.ObjectPointer) (block.QualifiedKey, error) {
	qualifiedKey, err := block.ResolveNamespace(obj.StorageNamespace, obj.Identifier)
	if err != nil {
		return qualifiedKey, err
	}
	if qualifiedKey.StorageType != block.StorageTypeLocal {
		return qualifiedKey, block.ErrInvalidNamespace
	}
	return qualifiedKey, nil
}

func (l *Adapter) getPath(identifier block.ObjectPointer) (string, error) {
	obj, err := resolveNamespace(identifier)
	if err != nil {
		return "", err
	}
	p := path.Join(l.path, obj.StorageNamespace, obj.Key)
	return p, nil
}

// maybeMkdir runs f(path), but if f fails due to file-not-found MkdirAll's its dir and then
// runs it again.
func maybeMkdir(path string, f func(p string) (*os.File, error)) (*os.File, error) {
	ret, err := f(path)
	if !errors.Is(err, os.ErrNotExist) {
		return ret, err
	}
	d := filepath.Dir(filepath.Clean(path))
	if err = os.MkdirAll(d, 0750); err != nil {
		return nil, err
	}
	return f(path)
}

func (l *Adapter) Put(obj block.ObjectPointer, _ int64, reader io.Reader, _ block.PutOpts) error {
	p, err := l.getPath(obj)
	if err != nil {
		return err
	}
	p = filepath.Clean(p)
	f, err := maybeMkdir(p, os.Create)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	_, err = io.Copy(f, reader)
	return err
}

func (l *Adapter) Remove(obj block.ObjectPointer) error {
	p, err := l.getPath(obj)
	if err != nil {
		return err
	}
	p = filepath.Clean(p)
	return os.Remove(p)
}

func (l *Adapter) Copy(sourceObj, destinationObj block.ObjectPointer) error {
	source, err := l.getPath(sourceObj)
	if err != nil {
		return err
	}
	sourceFile, err := os.Open(filepath.Clean(source))
	defer func() {
		_ = sourceFile.Close()
	}()
	if err != nil {
		return err
	}
	dest, err := l.getPath(destinationObj)
	if err != nil {
		return err
	}
	destinationFile, err := maybeMkdir(dest, os.Create)
	if err != nil {
		return err
	}
	defer func() {
		_ = destinationFile.Close()
	}()
	_, err = io.Copy(destinationFile, sourceFile)
	return err
}

func (l *Adapter) Get(obj block.ObjectPointer, _ int64) (reader io.ReadCloser, err error) {
	p, err := l.getPath(obj)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Clean(p), os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (l *Adapter) Exists(obj block.ObjectPointer) (bool, error) {
	p, err := l.getPath(obj)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (l *Adapter) GetRange(obj block.ObjectPointer, start int64, end int64) (io.ReadCloser, error) {
	p, err := l.getPath(obj)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Clean(p))
	if err != nil {
		return nil, err
	}
	return &struct {
		io.Reader
		io.Closer
	}{
		Reader: io.NewSectionReader(f, start, end-start+1),
		Closer: f,
	}, nil
}

func (l *Adapter) GetProperties(obj block.ObjectPointer) (block.Properties, error) {
	p, err := l.getPath(obj)
	if err != nil {
		return block.Properties{}, err
	}
	_, err = os.Stat(p)
	if err != nil {
		return block.Properties{}, err
	}
	// No properties, just return that it exists
	return block.Properties{}, nil
}

func isDirectoryWritable(pth string) bool {
	// test ability to write to directory.
	// as there is no simple way to test this in windows, I prefer the "brute force" method
	// of creating s dummy file. will work in any OS.
	// speed is not an issue, as this will be activated very few times during startup
	f, err := ioutil.TempFile(pth, "dummy")
	if err != nil {
		return false
	}
	_ = f.Close()
	_ = os.Remove(f.Name())
	return true
}

func (l *Adapter) CreateMultiPartUpload(obj block.ObjectPointer, _ *http.Request, _ block.CreateMultiPartUploadOpts) (string, error) {
	if strings.Contains(obj.Identifier, "/") {
		fullPath, err := l.getPath(obj)
		if err != nil {
			return "", err
		}
		fullDir := path.Dir(fullPath)
		err = os.MkdirAll(fullDir, 0750)
		if err != nil {
			return "", err
		}
	}
	uidBytes := uuid.New()
	uploadID := hex.EncodeToString(uidBytes[:])
	uploadID = l.uploadIDTranslator.SetUploadID(uploadID)
	return uploadID, nil
}

func (l *Adapter) UploadPart(obj block.ObjectPointer, _ int64, reader io.Reader, uploadID string, partNumber int64) (string, error) {
	if err := isValidUploadID(uploadID); err != nil {
		return "", err
	}
	md5Read := block.NewHashingReader(reader, block.HashFunctionMD5)
	fName := uploadID + fmt.Sprintf("-%05d", partNumber)
	err := l.Put(block.ObjectPointer{StorageNamespace: obj.StorageNamespace, Identifier: fName}, -1, md5Read, block.PutOpts{})
	etag := "\"" + hex.EncodeToString(md5Read.Md5.Sum(nil)) + "\""
	return etag, err
}

func (l *Adapter) AbortMultiPartUpload(obj block.ObjectPointer, uploadID string) error {
	if err := isValidUploadID(uploadID); err != nil {
		return err
	}
	files, err := l.getPartFiles(uploadID, obj)
	if err != nil {
		return err
	}
	l.removePartFiles(files)
	return nil
}

func (l *Adapter) CompleteMultiPartUpload(obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*string, int64, error) {
	if err := isValidUploadID(uploadID); err != nil {
		return nil, -1, err
	}
	etag := computeETag(multipartList.Part) + "-" + strconv.Itoa(len(multipartList.Part))
	partFiles, err := l.getPartFiles(uploadID, obj)
	if err != nil {
		return nil, -1, fmt.Errorf("part files not found for %s: %w", uploadID, err)
	}
	size, err := l.unitePartFiles(obj, partFiles)
	if err != nil {
		return nil, -1, fmt.Errorf("multipart upload unite for %s: %w", uploadID, err)
	}
	l.removePartFiles(partFiles)
	return &etag, size, nil
}

func computeETag(parts []*s3.CompletedPart) string {
	var etagHex []string
	for _, p := range parts {
		e := *p.ETag
		if strings.HasPrefix(e, "\"") && strings.HasSuffix(e, "\"") {
			e = e[1 : len(e)-1]
		}
		etagHex = append(etagHex, e)
	}
	s := strings.Join(etagHex, "")
	b, _ := hex.DecodeString(s)
	md5res := md5.Sum(b) //nolint:gosec
	csm := hex.EncodeToString(md5res[:])
	return csm
}

func (l *Adapter) unitePartFiles(identifier block.ObjectPointer, files []string) (int64, error) {
	p, err := l.getPath(identifier)
	if err != nil {
		return 0, err
	}
	unitedFile, err := os.Create(p)
	if err != nil {
		return 0, fmt.Errorf("create path %s: %w", p, err)
	}
	defer func() {
		_ = unitedFile.Close()
	}()
	var readers = []io.Reader{}
	for _, name := range files {
		f, err := os.Open(filepath.Clean(name))
		if err != nil {
			return 0, fmt.Errorf("open file %s: %w", name, err)
		}
		readers = append(readers, f)
		defer func() {
			_ = f.Close()
		}()
	}
	unitedReader := io.MultiReader(readers...)
	size, err := io.Copy(unitedFile, unitedReader)
	return size, err
}

func (l *Adapter) removePartFiles(files []string) {
	for _, name := range files {
		_ = os.Remove(name)
	}
}

func (l *Adapter) getPartFiles(uploadID string, obj block.ObjectPointer) ([]string, error) {
	newObj := block.ObjectPointer{
		StorageNamespace: obj.StorageNamespace,
		Identifier:       uploadID,
	}
	globPathPattern, err := l.getPath(newObj)
	if err != nil {
		return nil, err
	}
	globPathPattern += "*"
	names, err := filepath.Glob(globPathPattern)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func (l *Adapter) ValidateConfiguration(_ string) error {
	return nil
}

func (l *Adapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string, _ bool, _ []string) (block.Inventory, error) {
	return nil, ErrInventoryNotSupported
}

func (l *Adapter) BlockstoreType() string {
	return BlockstoreType
}

func isValidUploadID(uploadID string) error {
	_, err := hex.DecodeString(uploadID)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidUploadIDFormat, err)
	}
	return nil
}
