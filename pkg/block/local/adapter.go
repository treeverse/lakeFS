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
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
)

const BlockstoreType = "local"

type Adapter struct {
	path               string
	uploadIDTranslator block.UploadIDTranslator
	removeEmptyDir     bool
}

var (
	ErrPathNotWritable       = errors.New("path provided is not writable")
	ErrInventoryNotSupported = errors.New("inventory feature not implemented for local storage adapter")
	ErrInvalidUploadIDFormat = errors.New("invalid upload id format")
)

func WithTranslator(t block.UploadIDTranslator) func(a *Adapter) {
	return func(a *Adapter) {
		a.uploadIDTranslator = t
	}
}

func WithRemoveEmptyDir(b bool) func(a *Adapter) {
	return func(a *Adapter) {
		a.removeEmptyDir = b
	}
}

func NewAdapter(path string, opts ...func(a *Adapter)) (*Adapter, error) {
	path = filepath.Clean(path)
	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, err
	}
	if !isDirectoryWritable(path) {
		return nil, ErrPathNotWritable
	}
	adapter := &Adapter{
		path:               path,
		uploadIDTranslator: &block.NoOpTranslator{},
		removeEmptyDir:     true,
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

func (l *Adapter) Path() string {
	return l.path
}

func (l *Adapter) Put(_ context.Context, obj block.ObjectPointer, _ int64, reader io.Reader, _ block.PutOpts) error {
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

func (l *Adapter) Remove(_ context.Context, obj block.ObjectPointer) error {
	p, err := l.getPath(obj)
	if err != nil {
		return err
	}
	p = filepath.Clean(p)
	err = os.Remove(p)
	if err != nil {
		return err
	}
	if l.removeEmptyDir {
		dir := filepath.Dir(p)
		removeEmptyDirUntil(dir, l.path)
	}
	return nil
}

func removeEmptyDirUntil(dir string, stopAt string) {
	if stopAt == "" {
		return
	}
	if !strings.HasSuffix(stopAt, "/") {
		stopAt += "/"
	}
	for strings.HasPrefix(dir, stopAt) && dir != stopAt {
		err := os.Remove(dir)
		if err != nil {
			break
		}
		dir = filepath.Dir(dir)
		if dir == "/" {
			break
		}
	}
}

func (l *Adapter) Copy(_ context.Context, sourceObj, destinationObj block.ObjectPointer) error {
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

func (l *Adapter) UploadCopyPart(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int64) (string, error) {
	if err := isValidUploadID(uploadID); err != nil {
		return "", err
	}
	r, err := l.Get(ctx, sourceObj, 0)
	if err != nil {
		return "", err
	}
	md5Read := block.NewHashingReader(r, block.HashFunctionMD5)
	fName := uploadID + fmt.Sprintf("-%05d", partNumber)
	err = l.Put(ctx, block.ObjectPointer{StorageNamespace: destinationObj.StorageNamespace, Identifier: fName}, -1, md5Read, block.PutOpts{})
	etag := "\"" + hex.EncodeToString(md5Read.Md5.Sum(nil)) + "\""
	return etag, err
}

func (l *Adapter) UploadCopyPartRange(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber, startPosition, endPosition int64) (string, error) {
	if err := isValidUploadID(uploadID); err != nil {
		return "", err
	}
	r, err := l.GetRange(ctx, sourceObj, startPosition, endPosition)
	if err != nil {
		return "", err
	}
	md5Read := block.NewHashingReader(r, block.HashFunctionMD5)
	fName := uploadID + fmt.Sprintf("-%05d", partNumber)
	err = l.Put(ctx, block.ObjectPointer{StorageNamespace: destinationObj.StorageNamespace, Identifier: fName}, -1, md5Read, block.PutOpts{})
	etag := "\"" + hex.EncodeToString(md5Read.Md5.Sum(nil)) + "\""
	return etag, err
}

func (l *Adapter) Get(_ context.Context, obj block.ObjectPointer, _ int64) (reader io.ReadCloser, err error) {
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

func (l *Adapter) Walk(_ context.Context, walkOpt block.WalkOpts, walkFn block.WalkFunc) error {
	p := filepath.Clean(path.Join(l.path, walkOpt.StorageNamespace, walkOpt.Prefix))
	return filepath.Walk(p, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		return walkFn(p)
	})
}

func (l *Adapter) Exists(_ context.Context, obj block.ObjectPointer) (bool, error) {
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

func (l *Adapter) GetRange(_ context.Context, obj block.ObjectPointer, start int64, end int64) (io.ReadCloser, error) {
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

func (l *Adapter) GetProperties(_ context.Context, obj block.ObjectPointer) (block.Properties, error) {
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

func (l *Adapter) CreateMultiPartUpload(_ context.Context, obj block.ObjectPointer, _ *http.Request, _ block.CreateMultiPartUploadOpts) (string, error) {
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

func (l *Adapter) UploadPart(ctx context.Context, obj block.ObjectPointer, _ int64, reader io.Reader, uploadID string, partNumber int64) (string, error) {
	if err := isValidUploadID(uploadID); err != nil {
		return "", err
	}
	md5Read := block.NewHashingReader(reader, block.HashFunctionMD5)
	fName := uploadID + fmt.Sprintf("-%05d", partNumber)
	err := l.Put(ctx, block.ObjectPointer{StorageNamespace: obj.StorageNamespace, Identifier: fName}, -1, md5Read, block.PutOpts{})
	etag := "\"" + hex.EncodeToString(md5Read.Md5.Sum(nil)) + "\""
	return etag, err
}

func (l *Adapter) AbortMultiPartUpload(_ context.Context, obj block.ObjectPointer, uploadID string) error {
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

func (l *Adapter) CompleteMultiPartUpload(_ context.Context, obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*string, int64, error) {
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

func (l *Adapter) ValidateConfiguration(_ context.Context, _ string) error {
	return nil
}

func (l *Adapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string, _ bool, _ []string) (block.Inventory, error) {
	return nil, ErrInventoryNotSupported
}

func (l *Adapter) BlockstoreType() string {
	return BlockstoreType
}

func (l *Adapter) GetStorageNamespaceInfo() block.StorageNamespaceInfo {
	return block.DefaultStorageNamespaceInfo(BlockstoreType)
}

func isValidUploadID(uploadID string) error {
	_, err := hex.DecodeString(uploadID)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidUploadIDFormat, err)
	}
	return nil
}
