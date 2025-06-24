package mem

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
)

var (
	ErrNoDataForKey       = fmt.Errorf("no data for key: %w", block.ErrDataNotFound)
	ErrMultiPartNotFound  = fmt.Errorf("multipart ID not found")
	ErrNoPropertiesForKey = fmt.Errorf("no properties for key")
)

type partInfo struct {
	data         []byte
	lastModified time.Time
	size         int64
}

type mpu struct {
	id        string
	objectKey string // the key (from getKey(obj))
	initiated time.Time
	parts     map[int]partInfo // part number -> partInfo
}

func newMPU(objectKey string) *mpu {
	uid := uuid.New()
	uploadID := hex.EncodeToString(uid[:])
	return &mpu{
		id:        uploadID,
		objectKey: objectKey,
		initiated: time.Now(),
		parts:     make(map[int]partInfo),
	}
}

// get returns the concatenated data of all parts in the mpu.
// not thread safe: lock around calls.
func (m *mpu) get() []byte {
	buf := bytes.NewBuffer(nil)
	keys := make([]int, 0, len(m.parts))
	for k := range m.parts {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, part := range keys {
		buf.Write(m.parts[part].data)
	}
	return buf.Bytes()
}

type Adapter struct {
	data       map[string]map[string][]byte
	mpu        map[string]map[string]*mpu
	properties map[string]map[string]block.Properties
	mutex      sync.RWMutex
}

func New(_ context.Context, opts ...func(a *Adapter)) *Adapter {
	a := &Adapter{
		data:       make(map[string]map[string][]byte),
		mpu:        make(map[string]map[string]*mpu),
		properties: make(map[string]map[string]block.Properties),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// calcETag calculates the ETag for the given data using MD5 hash,
// similar to AWS S3 ETag calculation
func calcETag(data []byte) string {
	etag := md5.Sum(data) //nolint:gosec
	return hex.EncodeToString(etag[:])
}

func getKey(obj block.ObjectPointer) string {
	qk, err := block.DefaultResolveNamespace(obj.StorageNamespace, obj.Identifier, obj.IdentifierType)
	if err != nil {
		// fallback: old behavior, but this should not happen in normal use
		return obj.Identifier
	}
	return qk.Format()
}

func (a *Adapter) getNoLock(obj block.ObjectPointer) ([]byte, error) {
	storageID := obj.StorageID
	key := getKey(obj)
	dataMap, ok := a.data[storageID]
	if !ok {
		return nil, ErrNoDataForKey
	}
	data, ok := dataMap[key]
	if !ok {
		return nil, ErrNoDataForKey
	}
	return data, nil
}

func (a *Adapter) Put(_ context.Context, obj block.ObjectPointer, _ int64, reader io.Reader, opts block.PutOpts) (*block.PutResponse, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return nil, err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	storageID := obj.StorageID
	key := getKey(obj)
	if a.data[storageID] == nil {
		a.data[storageID] = make(map[string][]byte)
	}
	if a.properties[storageID] == nil {
		a.properties[storageID] = make(map[string]block.Properties)
	}
	a.data[storageID][key] = data
	a.properties[storageID][key] = block.Properties(opts)
	return &block.PutResponse{}, nil
}

func (a *Adapter) Get(_ context.Context, obj block.ObjectPointer) (io.ReadCloser, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return nil, err
	}
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	data, err := a.getNoLock(obj)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func verifyObjectPointer(obj block.ObjectPointer) error {
	const prefix = block.BlockstoreTypeMem + "://"
	if obj.StorageNamespace == "" {
		if !strings.HasPrefix(obj.Identifier, prefix) {
			return fmt.Errorf("%s block adapter: %w identifier: %s", block.BlockstoreTypeMem, block.ErrInvalidAddress, obj.Identifier)
		}
	} else if !strings.HasPrefix(obj.StorageNamespace, prefix) {
		return fmt.Errorf("%s block adapter: %w storage namespace: %s", block.BlockstoreTypeMem, block.ErrInvalidAddress, obj.StorageNamespace)
	}
	return nil
}

func (a *Adapter) GetWalker(storageID string, opts block.WalkerOptions) (block.Walker, error) {
	if err := block.ValidateStorageType(opts.StorageURI, block.StorageTypeMem); err != nil {
		return nil, err
	}
	return NewMemWalker(storageID, a), nil
}

func (a *Adapter) GetPreSignedURL(_ context.Context, obj block.ObjectPointer, _ block.PreSignMode, _ string) (string, time.Time, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return "", time.Time{}, err
	}
	return "", time.Time{}, fmt.Errorf("get pre signed url: %w", block.ErrOperationNotSupported)
}

func (a *Adapter) Exists(_ context.Context, obj block.ObjectPointer) (bool, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return false, err
	}
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	storageID := obj.StorageID
	key := getKey(obj)
	dataMap, ok := a.data[storageID]
	if !ok {
		return false, nil
	}
	_, ok = dataMap[key]
	return ok, nil
}

func (a *Adapter) GetRange(_ context.Context, obj block.ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return nil, err
	}
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	storageID := obj.StorageID
	key := getKey(obj)
	dataMap, ok := a.data[storageID]
	if !ok {
		return nil, ErrNoDataForKey
	}
	data, ok := dataMap[key]
	if !ok {
		return nil, ErrNoDataForKey
	}
	return io.NopCloser(io.NewSectionReader(bytes.NewReader(data), startPosition, endPosition-startPosition+1)), nil
}

func (a *Adapter) GetProperties(_ context.Context, obj block.ObjectPointer) (block.Properties, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return block.Properties{}, err
	}
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	storageID := obj.StorageID
	key := getKey(obj)
	propsMap, ok := a.properties[storageID]
	if !ok {
		return block.Properties{}, ErrNoPropertiesForKey
	}
	props, ok := propsMap[key]
	if !ok {
		return block.Properties{}, ErrNoPropertiesForKey
	}
	return props, nil
}

func (a *Adapter) Remove(_ context.Context, obj block.ObjectPointer) error {
	if err := verifyObjectPointer(obj); err != nil {
		return err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	storageID := obj.StorageID
	key := getKey(obj)
	if a.data[storageID] != nil {
		delete(a.data[storageID], key)
	}
	if a.properties[storageID] != nil {
		delete(a.properties[storageID], key)
	}
	return nil
}

func (a *Adapter) Copy(_ context.Context, sourceObj, destinationObj block.ObjectPointer) error {
	if err := verifyObjectPointer(sourceObj); err != nil {
		return err
	}
	if err := verifyObjectPointer(destinationObj); err != nil {
		return err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	srcStorageID := sourceObj.StorageID
	dstStorageID := destinationObj.StorageID
	sourceKey := getKey(sourceObj)
	destinationKey := getKey(destinationObj)
	if a.data[srcStorageID] != nil {
		if a.data[dstStorageID] == nil {
			a.data[dstStorageID] = make(map[string][]byte)
		}
		a.data[dstStorageID][destinationKey] = a.data[srcStorageID][sourceKey]
	}
	if a.properties[srcStorageID] != nil {
		if a.properties[dstStorageID] == nil {
			a.properties[dstStorageID] = make(map[string]block.Properties)
		}
		a.properties[dstStorageID][destinationKey] = a.properties[srcStorageID][sourceKey]
	}
	return nil
}

func (a *Adapter) UploadPart(_ context.Context, obj block.ObjectPointer, _ int64, reader io.Reader, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return nil, err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	storageID := obj.StorageID
	if a.mpu[storageID] == nil {
		a.mpu[storageID] = make(map[string]*mpu)
	}
	mpu, ok := a.mpu[storageID][uploadID]
	if !ok {
		return nil, ErrMultiPartNotFound
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	etag := calcETag(data)
	mpu.parts[partNumber] = partInfo{
		data:         data,
		lastModified: time.Now(),
		size:         int64(len(data)),
	}
	return &block.UploadPartResponse{
		ETag: etag,
	}, nil
}

func (a *Adapter) UploadCopyPart(_ context.Context, sourceObj, _ block.ObjectPointer, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	if err := verifyObjectPointer(sourceObj); err != nil {
		return nil, err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	storageID := sourceObj.StorageID
	if a.mpu[storageID] == nil {
		a.mpu[storageID] = make(map[string]*mpu)
	}
	mpu, ok := a.mpu[storageID][uploadID]
	if !ok {
		return nil, ErrMultiPartNotFound
	}
	data, err := a.getNoLock(sourceObj)
	if err != nil {
		return nil, err
	}
	etag := calcETag(data)
	mpu.parts[partNumber] = partInfo{
		data:         data,
		lastModified: time.Now(),
		size:         int64(len(data)),
	}
	return &block.UploadPartResponse{
		ETag: etag,
	}, nil
}

func (a *Adapter) UploadCopyPartRange(_ context.Context, sourceObj, _ block.ObjectPointer, uploadID string, partNumber int, startPosition, endPosition int64) (*block.UploadPartResponse, error) {
	if err := verifyObjectPointer(sourceObj); err != nil {
		return nil, err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	storageID := sourceObj.StorageID
	if a.mpu[storageID] == nil {
		a.mpu[storageID] = make(map[string]*mpu)
	}
	mpu, ok := a.mpu[storageID][uploadID]
	if !ok {
		return nil, ErrMultiPartNotFound
	}
	dataMap, ok := a.data[storageID]
	if !ok {
		return nil, ErrNoDataForKey
	}
	key := getKey(sourceObj)
	data, ok := dataMap[key]
	if !ok {
		return nil, ErrNoDataForKey
	}
	reader := io.NewSectionReader(bytes.NewReader(data), startPosition, endPosition-startPosition+1)
	partData, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	etag := calcETag(partData)
	mpu.parts[partNumber] = partInfo{
		data:         partData,
		lastModified: time.Now(),
		size:         int64(len(partData)),
	}
	return &block.UploadPartResponse{
		ETag: etag,
	}, nil
}

func (a *Adapter) CreateMultiPartUpload(_ context.Context, obj block.ObjectPointer, _ *http.Request, _ block.CreateMultiPartUploadOpts) (*block.CreateMultiPartUploadResponse, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return nil, err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	storageID := obj.StorageID
	key := getKey(obj)
	if a.mpu[storageID] == nil {
		a.mpu[storageID] = make(map[string]*mpu)
	}
	mpu := newMPU(key)
	a.mpu[storageID][mpu.id] = mpu
	return &block.CreateMultiPartUploadResponse{
		UploadID: mpu.id,
	}, nil
}

func (a *Adapter) AbortMultiPartUpload(_ context.Context, obj block.ObjectPointer, uploadID string) error {
	if err := verifyObjectPointer(obj); err != nil {
		return err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	storageID := obj.StorageID
	if a.mpu[storageID] == nil {
		return ErrMultiPartNotFound
	}
	_, ok := a.mpu[storageID][uploadID]
	if !ok {
		return ErrMultiPartNotFound
	}
	delete(a.mpu[storageID], uploadID)
	return nil
}

func (a *Adapter) CompleteMultiPartUpload(_ context.Context, obj block.ObjectPointer, uploadID string, _ *block.MultipartUploadCompletion) (*block.CompleteMultiPartUploadResponse, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return nil, err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	storageID := obj.StorageID
	if a.mpu[storageID] == nil {
		return nil, ErrMultiPartNotFound
	}
	mpu, ok := a.mpu[storageID][uploadID]
	if !ok {
		return nil, ErrMultiPartNotFound
	}
	data := mpu.get()
	key := getKey(obj)
	if a.data[storageID] == nil {
		a.data[storageID] = make(map[string][]byte)
	}
	a.data[storageID][key] = data
	delete(a.mpu[storageID], uploadID) // delete the mpu after completion
	etag := calcETag(data)
	return &block.CompleteMultiPartUploadResponse{
		ETag:          etag,
		ContentLength: int64(len(data)),
	}, nil
}

func (a *Adapter) BlockstoreType() string {
	return block.BlockstoreTypeMem
}

func (a *Adapter) BlockstoreMetadata(_ context.Context) (*block.BlockstoreMetadata, error) {
	return nil, fmt.Errorf("blockstore metadata: %w", block.ErrOperationNotSupported)
}

func (a *Adapter) GetStorageNamespaceInfo(string) *block.StorageNamespaceInfo {
	info := block.DefaultStorageNamespaceInfo(block.BlockstoreTypeMem)
	info.PreSignSupport = false
	info.ImportSupport = false
	return &info
}

func (a *Adapter) ResolveNamespace(_, storageNamespace, key string, identifierType block.IdentifierType) (block.QualifiedKey, error) {
	return block.DefaultResolveNamespace(storageNamespace, key, identifierType)
}

func (a *Adapter) GetRegion(_ context.Context, _, _ string) (string, error) {
	return "", fmt.Errorf("get region: %w", block.ErrOperationNotSupported)
}

func (a *Adapter) RuntimeStats() map[string]string {
	return nil
}

func (a *Adapter) GetPresignUploadPartURL(_ context.Context, _ block.ObjectPointer, _ string, _ int) (string, error) {
	return "", fmt.Errorf("get presign upload part url: %w", block.ErrOperationNotSupported)
}

func (a *Adapter) ListParts(_ context.Context, obj block.ObjectPointer, uploadID string, opts block.ListPartsOpts) (*block.ListPartsResponse, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	storageID := obj.StorageID
	if a.mpu[storageID] == nil {
		return nil, ErrMultiPartNotFound
	}
	mpu, ok := a.mpu[storageID][uploadID]
	if !ok {
		return nil, ErrMultiPartNotFound
	}
	// Collect part numbers and sort
	partNumbers := make([]int, 0, len(mpu.parts))
	for pn := range mpu.parts {
		partNumbers = append(partNumbers, pn)
	}
	sort.Ints(partNumbers)
	// Pagination
	startIdx := 0
	if opts.PartNumberMarker != nil {
		marker, err := strconv.Atoi(*opts.PartNumberMarker)
		if err != nil {
			return nil, fmt.Errorf("invalid part number marker: %w", err)
		}
		for i, pn := range partNumbers {
			if pn > marker {
				startIdx = i
				break
			}
		}
	}
	maxParts := len(partNumbers)
	if opts.MaxParts != nil && int(*opts.MaxParts) < maxParts-startIdx {
		maxParts = startIdx + int(*opts.MaxParts)
	}
	isTruncated := false
	var nextPartNumberMarker *string
	if maxParts < len(partNumbers) {
		isTruncated = true
		nextMarker := strconv.Itoa(partNumbers[maxParts-1])
		nextPartNumberMarker = &nextMarker
	}
	parts := make([]block.MultipartPart, 0, maxParts-startIdx)
	for _, pn := range partNumbers[startIdx:maxParts] {
		p := mpu.parts[pn]
		etag := calcETag(p.data)
		parts = append(parts, block.MultipartPart{
			ETag:         etag,
			PartNumber:   pn,
			LastModified: p.lastModified,
			Size:         p.size,
		})
	}
	return &block.ListPartsResponse{
		Parts:                parts,
		NextPartNumberMarker: nextPartNumberMarker,
		IsTruncated:          isTruncated,
	}, nil
}

func (a *Adapter) ListMultipartUploads(_ context.Context, obj block.ObjectPointer, opts block.ListMultipartUploadsOpts) (*block.ListMultipartUploadsResponse, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	storageID := obj.StorageID
	type mpuEntry struct {
		id  string
		mpu *mpu
	}
	mpuList := make([]mpuEntry, 0)
	if a.mpu[storageID] != nil {
		for id, m := range a.mpu[storageID] {
			mpuList = append(mpuList, mpuEntry{id, m})
		}
	}
	// Sort by key, then upload ID (lexicographically)
	sort.Slice(mpuList, func(i, j int) bool {
		if mpuList[i].mpu.objectKey == mpuList[j].mpu.objectKey {
			return mpuList[i].id < mpuList[j].id
		}
		return mpuList[i].mpu.objectKey < mpuList[j].mpu.objectKey
	})
	// Pagination
	startIdx := 0
	if opts.KeyMarker != nil && *opts.KeyMarker != "" {
		for i, entry := range mpuList {
			if entry.mpu.objectKey > *opts.KeyMarker {
				startIdx = i
				break
			}
			if entry.mpu.objectKey == *opts.KeyMarker {
				// If UploadIDMarker is set, skip up to and including that upload ID
				if opts.UploadIDMarker != nil && *opts.UploadIDMarker != "" {
					if entry.id > *opts.UploadIDMarker {
						startIdx = i
						break
					}
					if entry.id == *opts.UploadIDMarker {
						startIdx = i + 1
					}
				} else {
					// If only KeyMarker is set, skip all uploads for that key
					startIdx = i + 1
				}
			}
		}
	}
	maxUploads := len(mpuList)
	if opts.MaxUploads != nil && int(*opts.MaxUploads) < maxUploads-startIdx {
		maxUploads = startIdx + int(*opts.MaxUploads)
	}
	isTruncated := false
	var nextKeyMarker, nextUploadIDMarker *string
	if maxUploads < len(mpuList) {
		isTruncated = true
		nextKeyMarker = &mpuList[maxUploads-1].mpu.objectKey
		nextUploadIDMarker = &mpuList[maxUploads-1].id
	}
	uploads := make([]types.MultipartUpload, 0, maxUploads-startIdx)
	for _, entry := range mpuList[startIdx:maxUploads] {
		uploads = append(uploads, types.MultipartUpload{
			UploadId:  &entry.id,
			Key:       &entry.mpu.objectKey,
			Initiated: &entry.mpu.initiated,
		})
	}
	return &block.ListMultipartUploadsResponse{
		Uploads:            uploads,
		NextKeyMarker:      nextKeyMarker,
		NextUploadIDMarker: nextUploadIDMarker,
		IsTruncated:        isTruncated,
		MaxUploads:         opts.MaxUploads,
	}, nil
}
