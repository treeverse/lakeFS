package local

import (
	"context"
	"crypto/md5" //nolint:gosec
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/logging"
	"golang.org/x/exp/slices"
)

const StoragePrefix = block.BlockstoreTypeLocal + "://"

type Adapter struct {
	path                    string
	removeEmptyDir          bool
	allowedExternalPrefixes []string
	importEnabled           bool
}

const (
	DefaultNamespacePrefix = "local:/"
)

var (
	ErrPathNotWritable       = errors.New("path provided is not writable")
	ErrInventoryNotSupported = errors.New("inventory feature not implemented for local storage adapter")
	ErrInvalidUploadIDFormat = errors.New("invalid upload id format")
	ErrBadPath               = errors.New("bad path traversal blocked")
)

type QualifiedKey struct {
	block.CommonQualifiedKey
	path string
}

func (qk QualifiedKey) Format() string {
	p := path.Join(qk.path, qk.GetStorageNamespace(), qk.GetKey())
	return qk.GetStorageType().Scheme() + "://" + p
}

func (qk QualifiedKey) GetStorageType() block.StorageType {
	return qk.CommonQualifiedKey.GetStorageType()
}

func (qk QualifiedKey) GetStorageNamespace() string {
	return qk.CommonQualifiedKey.GetStorageNamespace()
}

func (qk QualifiedKey) GetKey() string {
	return qk.CommonQualifiedKey.GetKey()
}

func WithAllowedExternalPrefixes(prefixes []string) func(a *Adapter) {
	return func(a *Adapter) {
		a.allowedExternalPrefixes = prefixes
	}
}

func WithImportEnabled(b bool) func(a *Adapter) {
	return func(a *Adapter) {
		a.importEnabled = b
	}
}

func WithRemoveEmptyDir(b bool) func(a *Adapter) {
	return func(a *Adapter) {
		a.removeEmptyDir = b
	}
}

func NewAdapter(path string, opts ...func(a *Adapter)) (*Adapter, error) {
	// Clean() the path so that misconfiguration does not allow path traversal.
	path = filepath.Clean(path)
	err := os.MkdirAll(path, 0o700) //nolint: gomnd
	if err != nil {
		return nil, err
	}
	if !isDirectoryWritable(path) {
		return nil, ErrPathNotWritable
	}
	localAdapter := &Adapter{
		path:           path,
		removeEmptyDir: true,
	}
	for _, opt := range opts {
		opt(localAdapter)
	}
	return localAdapter, nil
}

func (l *Adapter) GetPreSignedURL(_ context.Context, _ block.ObjectPointer, _ block.PreSignMode) (string, error) {
	return "", fmt.Errorf("local adapter: %w", block.ErrOperationNotSupported)
}

// verifyRelPath ensures that p is under the directory controlled by this adapter.  It does not
// examine the filesystem and can mistakenly error out when symbolic links are involved.
func (l *Adapter) verifyRelPath(p string) error {
	if !strings.HasPrefix(filepath.Clean(p), l.path) {
		return fmt.Errorf("%s: %w", p, ErrBadPath)
	}
	return nil
}

func (l *Adapter) extractParamsFromObj(ptr block.ObjectPointer) (string, error) {
	if strings.HasPrefix(ptr.Identifier, StoragePrefix) {
		// check abs path
		p := ptr.Identifier[len(StoragePrefix):]
		if err := VerifyAbsPath(p, l.path, l.allowedExternalPrefixes); err != nil {
			return "", err
		}
		return p, nil
	}
	// relative path
	if !strings.HasPrefix(ptr.StorageNamespace, StoragePrefix) {
		return "", fmt.Errorf("%w: storage namespace", ErrBadPath)
	}
	p := path.Join(l.path, ptr.StorageNamespace[len(StoragePrefix):], ptr.Identifier)
	if err := l.verifyRelPath(p); err != nil {
		return "", err
	}
	return p, nil
}

// maybeMkdir verifies path is allowed and runs f(path), but if f fails due to file-not-found
// MkdirAll's its dir and then runs it again.
func (l *Adapter) maybeMkdir(path string, f func(p string) (*os.File, error)) (*os.File, error) {
	if err := l.verifyRelPath(path); err != nil {
		return nil, err
	}
	ret, err := f(path)
	if !errors.Is(err, os.ErrNotExist) {
		return ret, err
	}
	d := filepath.Dir(filepath.Clean(path))
	if err = os.MkdirAll(d, 0o750); err != nil { //nolint: gomnd
		return nil, err
	}
	return f(path)
}

func (l *Adapter) Path() string {
	return l.path
}

func (l *Adapter) Put(_ context.Context, obj block.ObjectPointer, _ int64, reader io.Reader, _ block.PutOpts) error {
	p, err := l.extractParamsFromObj(obj)
	if err != nil {
		return err
	}
	p = filepath.Clean(p)
	f, err := l.maybeMkdir(p, os.Create)
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
	p, err := l.extractParamsFromObj(obj)
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
		repoRoot := obj.StorageNamespace[len(StoragePrefix):]
		removeEmptyDirUntil(dir, path.Join(l.path, repoRoot))
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
	source, err := l.extractParamsFromObj(sourceObj)
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
	dest, err := l.extractParamsFromObj(destinationObj)
	if err != nil {
		return err
	}
	destinationFile, err := l.maybeMkdir(dest, os.Create)
	if err != nil {
		return err
	}
	defer func() {
		_ = destinationFile.Close()
	}()
	_, err = io.Copy(destinationFile, sourceFile)
	return err
}

func (l *Adapter) UploadCopyPart(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	if err := isValidUploadID(uploadID); err != nil {
		return nil, err
	}
	r, err := l.Get(ctx, sourceObj, 0)
	if err != nil {
		return nil, err
	}
	md5Read := block.NewHashingReader(r, block.HashFunctionMD5)
	fName := uploadID + fmt.Sprintf("-%05d", partNumber)
	err = l.Put(ctx, block.ObjectPointer{StorageNamespace: destinationObj.StorageNamespace, Identifier: fName}, -1, md5Read, block.PutOpts{})
	if err != nil {
		return nil, err
	}
	etag := hex.EncodeToString(md5Read.Md5.Sum(nil))
	return &block.UploadPartResponse{
		ETag: etag,
	}, nil
}

func (l *Adapter) UploadCopyPartRange(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int, startPosition, endPosition int64) (*block.UploadPartResponse, error) {
	if err := isValidUploadID(uploadID); err != nil {
		return nil, err
	}
	r, err := l.GetRange(ctx, sourceObj, startPosition, endPosition)
	if err != nil {
		return nil, err
	}
	md5Read := block.NewHashingReader(r, block.HashFunctionMD5)
	fName := uploadID + fmt.Sprintf("-%05d", partNumber)
	err = l.Put(ctx, block.ObjectPointer{StorageNamespace: destinationObj.StorageNamespace, Identifier: fName}, -1, md5Read, block.PutOpts{})
	if err != nil {
		return nil, err
	}
	etag := hex.EncodeToString(md5Read.Md5.Sum(nil))
	return &block.UploadPartResponse{
		ETag: etag,
	}, err
}

func (l *Adapter) Get(_ context.Context, obj block.ObjectPointer, _ int64) (reader io.ReadCloser, err error) {
	p, err := l.extractParamsFromObj(obj)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Clean(p), os.O_RDONLY, 0o600) //nolint: gomnd
	if os.IsNotExist(err) {
		return nil, block.ErrDataNotFound
	}
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (l *Adapter) GetWalker(uri *url.URL) (block.Walker, error) {
	if err := block.ValidateStorageType(uri, block.StorageTypeLocal); err != nil {
		return nil, err
	}

	err := VerifyAbsPath(uri.Path, l.path, l.allowedExternalPrefixes)
	if err != nil {
		return nil, err
	}
	return NewLocalWalker(params.Local{
		Path:                    l.path,
		ImportEnabled:           l.importEnabled,
		AllowedExternalPrefixes: l.allowedExternalPrefixes,
	}), nil
}

func (l *Adapter) Exists(_ context.Context, obj block.ObjectPointer) (bool, error) {
	p, err := l.extractParamsFromObj(obj)
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
	if start < 0 || end < start {
		return nil, block.ErrBadIndex
	}
	p, err := l.extractParamsFromObj(obj)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Clean(p))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, block.ErrDataNotFound
		}
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
	p, err := l.extractParamsFromObj(obj)
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

// isDirectoryWritable tests that pth, which must not be controllable by user input, is a
// writable directory.  As there is no simple way to test this in windows, I prefer the "brute
// force" method of creating s dummy file.  Will work in any OS.  speed is not an issue, as
// this will be activated very few times during startup.
func isDirectoryWritable(pth string) bool {
	f, err := os.CreateTemp(pth, "dummy")
	if err != nil {
		return false
	}
	_ = f.Close()
	_ = os.Remove(f.Name())
	return true
}

func (l *Adapter) CreateMultiPartUpload(_ context.Context, obj block.ObjectPointer, _ *http.Request, _ block.CreateMultiPartUploadOpts) (*block.CreateMultiPartUploadResponse, error) {
	if strings.Contains(obj.Identifier, "/") {
		fullPath, err := l.extractParamsFromObj(obj)
		if err != nil {
			return nil, err
		}
		fullDir := path.Dir(fullPath)
		err = os.MkdirAll(fullDir, 0o750) //nolint: gomnd
		if err != nil {
			return nil, err
		}
	}
	uidBytes := uuid.New()
	uploadID := hex.EncodeToString(uidBytes[:])
	return &block.CreateMultiPartUploadResponse{
		UploadID: uploadID,
	}, nil
}

func (l *Adapter) UploadPart(ctx context.Context, obj block.ObjectPointer, _ int64, reader io.Reader, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	if err := isValidUploadID(uploadID); err != nil {
		return nil, err
	}
	md5Read := block.NewHashingReader(reader, block.HashFunctionMD5)
	fName := uploadID + fmt.Sprintf("-%05d", partNumber)
	err := l.Put(ctx, block.ObjectPointer{StorageNamespace: obj.StorageNamespace, Identifier: fName}, -1, md5Read, block.PutOpts{})
	etag := hex.EncodeToString(md5Read.Md5.Sum(nil))
	return &block.UploadPartResponse{
		ETag: etag,
	}, err
}

func (l *Adapter) AbortMultiPartUpload(_ context.Context, obj block.ObjectPointer, uploadID string) error {
	if err := isValidUploadID(uploadID); err != nil {
		return err
	}
	files, err := l.getPartFiles(uploadID, obj)
	if err != nil {
		return err
	}
	if err = l.removePartFiles(files); err != nil {
		return err
	}
	return nil
}

func (l *Adapter) CompleteMultiPartUpload(_ context.Context, obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*block.CompleteMultiPartUploadResponse, error) {
	if err := isValidUploadID(uploadID); err != nil {
		return nil, err
	}
	etag := computeETag(multipartList.Part) + "-" + strconv.Itoa(len(multipartList.Part))
	partFiles, err := l.getPartFiles(uploadID, obj)
	if err != nil {
		return nil, fmt.Errorf("part files not found for %s: %w", uploadID, err)
	}
	size, err := l.unitePartFiles(obj, partFiles)
	if err != nil {
		return nil, fmt.Errorf("multipart upload unite for %s: %w", uploadID, err)
	}
	if err = l.removePartFiles(partFiles); err != nil {
		return nil, err
	}
	return &block.CompleteMultiPartUploadResponse{
		ETag:          etag,
		ContentLength: size,
	}, nil
}

func computeETag(parts []block.MultipartPart) string {
	var etagHex []string
	for _, p := range parts {
		e := strings.Trim(p.ETag, `"`)
		etagHex = append(etagHex, e)
	}
	s := strings.Join(etagHex, "")
	b, _ := hex.DecodeString(s)
	md5res := md5.Sum(b) //nolint:gosec
	csm := hex.EncodeToString(md5res[:])
	return csm
}

func (l *Adapter) unitePartFiles(identifier block.ObjectPointer, filenames []string) (int64, error) {
	p, err := l.extractParamsFromObj(identifier)
	if err != nil {
		return 0, err
	}
	unitedFile, err := os.Create(p)
	if err != nil {
		return 0, fmt.Errorf("create path %s: %w", p, err)
	}
	files := make([]*os.File, 0, len(filenames))
	defer func() {
		_ = unitedFile.Close()
		for _, f := range files {
			_ = f.Close()
		}
	}()
	for _, name := range filenames {
		if err := l.verifyRelPath(name); err != nil {
			return 0, err
		}
		f, err := os.Open(filepath.Clean(name))
		if err != nil {
			return 0, fmt.Errorf("open file %s: %w", name, err)
		}
		files = append(files, f)
	}
	// convert slice file files to readers
	readers := make([]io.Reader, len(files))
	for i := range files {
		readers[i] = files[i]
	}
	unitedReader := io.MultiReader(readers...)
	return io.Copy(unitedFile, unitedReader)
}

func (l *Adapter) removePartFiles(files []string) error {
	var firstErr error
	for _, name := range files {
		if err := l.verifyRelPath(name); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
		// If removal fails prefer to skip the error: "only" wasted space.
		_ = os.Remove(name)
	}
	return firstErr
}

func (l *Adapter) getPartFiles(uploadID string, obj block.ObjectPointer) ([]string, error) {
	newObj := block.ObjectPointer{
		StorageNamespace: obj.StorageNamespace,
		Identifier:       uploadID,
	}
	globPathPattern, err := l.extractParamsFromObj(newObj)
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

func (l *Adapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string, _ bool, _ []string) (block.Inventory, error) {
	return nil, ErrInventoryNotSupported
}

func (l *Adapter) BlockstoreType() string {
	return block.BlockstoreTypeLocal
}

func (l *Adapter) GetStorageNamespaceInfo() block.StorageNamespaceInfo {
	info := block.DefaultStorageNamespaceInfo(block.BlockstoreTypeLocal)
	info.PreSignSupport = false
	info.DefaultNamespacePrefix = DefaultNamespacePrefix
	info.ImportSupport = l.importEnabled
	return info
}

func (l *Adapter) ResolveNamespace(storageNamespace, key string, identifierType block.IdentifierType) (block.QualifiedKey, error) {
	qk, err := block.DefaultResolveNamespace(storageNamespace, key, identifierType)
	if err != nil {
		return nil, err
	}

	// Check if path allowed and return error if path is not allowed
	_, err = l.extractParamsFromObj(block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       key,
		IdentifierType:   identifierType,
	})
	if err != nil {
		return nil, err
	}

	return QualifiedKey{
		CommonQualifiedKey: qk,
		path:               l.path,
	}, nil
}

func (l *Adapter) RuntimeStats() map[string]string {
	return nil
}

func VerifyAbsPath(absPath, adapterPath string, allowedPrefixes []string) error {
	// check we have a valid abs path
	if !path.IsAbs(absPath) || path.Clean(absPath) != absPath {
		return ErrBadPath
	}
	// point to storage namespace
	if strings.HasPrefix(absPath, adapterPath) {
		return nil
	}
	// allowed places
	if !slices.ContainsFunc(allowedPrefixes, func(prefix string) bool {
		return strings.HasPrefix(absPath, prefix)
	}) {
		return block.ErrForbidden
	}
	return nil
}

func isValidUploadID(uploadID string) error {
	_, err := hex.DecodeString(uploadID)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidUploadIDFormat, err)
	}
	return nil
}
