package oss

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/distribution/distribution/v3/internal/dcontext"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/base"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
)

const (
	driverName         = "oss"
	minChunkSize       = 1024 * 1024
	chunkSize          = 10 * 1024 * 1024
	listMax      int32 = 1000
)

var _ storagedriver.StorageDriver = &driver{}

// driver is the core service for interacting with OSS
type driver struct {
	oss           *oss.Client
	pool          *sync.Pool // object buffer pool used to improve performance
	bucket        string
	rootDirectory string
	chunkSize     int64
}

type baseEmbed struct {
	base.Base
}

// Driver implements the storagedriver.StorageDriver interface
type Driver struct {
	baseEmbed
}

func init() {
	factory.Register(driverName, &ossDriverFactory{})
}

// ossDriverFactory is the factory for creating new driver
type ossDriverFactory struct{}

func (f *ossDriverFactory) Create(
	ctx context.Context,
	parameters map[string]interface{},
) (storagedriver.StorageDriver, error) {
	params, err := NewParameters(parameters)
	if err != nil {
		return nil, err
	}
	return New(ctx, params)
}

func New(ctx context.Context, params *Parameters) (*Driver, error) {
	// initialize OSS client
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			params.AccessKeyID, params.AccessKeySecret, "")).
		WithRegion(params.Region)

	client := oss.NewClient(cfg)

	d := &driver{
		oss: client,
		pool: &sync.Pool{
			New: func() any { return &bytes.Buffer{} },
		},
		bucket:        params.Bucket,
		chunkSize:     chunkSize,
		rootDirectory: params.RootDirectory,
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

func parseNotFoundError(path string, err error) error {
	var se *oss.ServiceError
	if errors.As(err, &se) {
		if se.StatusCode == http.StatusNotFound {
			return storagedriver.PathNotFoundError{Path: path}
		}
	}

	return err
}

// Implement the storagedriver.StorageDriver interface

// Name return the plugin name
func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	reader, err := d.Reader(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(reader)
}

func (d *driver) PutContent(ctx context.Context, path string, content []byte) error {
	_, err := d.oss.PutObject(ctx, &oss.PutObjectRequest{
		Bucket: d.bucketPtr(),
		Key:    d.pathToKeyPtr(path),
		Body:   bytes.NewReader(content),
	})
	return err
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	req := &oss.GetObjectRequest{
		Bucket: d.bucketPtr(),
		Key:    d.pathToKeyPtr(path),
		Range:  d.ossRangePtr(offset),
	}

	resp, err := d.oss.GetObject(ctx, req)
	if err != nil {
		var se *oss.ServiceError
		if errors.As(err, &se) {
			if se.StatusCode == http.StatusNotFound {
				return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
			}
			if se.StatusCode == http.StatusRequestedRangeNotSatisfiable {
				return io.NopCloser(bytes.NewReader(nil)), nil
			}
		}
		return nil, err
	}

	// Corner case. When the offset equals to the content length,
	// OSS returns the full object, but we want an empty reader
	if offset == resp.ContentLength {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	return resp.Body, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
// It only allows appending to paths with zero size committed content,
// in which the existing content is overridden with the new content.
// It returns storagedriver.Error when appending to paths
// with non-zero committed content.
func (d *driver) Writer(ctx context.Context, path string, appendMode bool) (storagedriver.FileWriter, error) {
	keyPtr := d.pathToKeyPtr(path)

	// new multipart upload
	init := func() (storagedriver.FileWriter, error) {
		res, err := d.oss.InitiateMultipartUpload(ctx, &oss.InitiateMultipartUploadRequest{
			Bucket: d.bucketPtr(),
			Key:    keyPtr,
		})
		if err != nil {
			return nil, err
		}
		return d.newWriter(ctx, *keyPtr, *res.UploadId, nil), nil
	}

	// new data
	if !appendMode {
		return init()
	}

	req := oss.ListMultipartUploadsRequest{
		Bucket: d.bucketPtr(),
		Prefix: keyPtr,
	}
	isTruncated := true
	for isTruncated {
		res, err := d.oss.ListMultipartUploads(ctx, &req)
		if err != nil {
			return nil, err
		}
		// res.Uploads can only be empty on the first call.
		// if there were no more results to return after the first call, res.IsTruncated would have been false
		// and the loop would be exited without recalling
		if len(res.Uploads) == 0 {
			fileInfo, err := d.Stat(ctx, path)
			if err != nil {
				return nil, err
			}

			if fileInfo.Size() == 0 { // new multipart upload
				return init()
			}
			return nil, storagedriver.Error{
				DriverName: driverName,
				Detail:     fmt.Errorf("append to zero-size path %s unsupported", path),
			}
		}

		var allParts []oss.Part
		for _, multi := range res.Uploads {
			if *keyPtr != *multi.Key {
				continue
			}

			v, err := d.oss.ListParts(ctx, &oss.ListPartsRequest{
				Bucket:   d.bucketPtr(),
				Key:      keyPtr,
				UploadId: multi.UploadId,
			})
			if err != nil {
				return nil, err
			}
			allParts = append(allParts, v.Parts...)
			for v.IsTruncated {
				v, err = d.oss.ListParts(ctx, &oss.ListPartsRequest{
					Bucket:           d.bucketPtr(),
					Key:              keyPtr,
					UploadId:         multi.UploadId,
					PartNumberMarker: v.NextPartNumberMarker,
				})
				if err != nil {
					return nil, err
				}
				allParts = append(allParts, v.Parts...)
			}
			return d.newWriter(ctx, *keyPtr, *multi.UploadId, allParts), nil
		}

		// next
		req.KeyMarker = res.NextKeyMarker
		req.UploadIdMarker = res.NextUploadIdMarker
		isTruncated = res.IsTruncated
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

func (d *driver) statHead(ctx context.Context, path string) (*storagedriver.FileInfoFields, error) {
	resp, err := d.oss.HeadObject(ctx, &oss.HeadObjectRequest{
		Bucket: d.bucketPtr(),
		Key:    d.pathToKeyPtr(path),
	})
	if err != nil {
		return nil, err
	}
	return &storagedriver.FileInfoFields{
		Path:    path,
		IsDir:   false,
		Size:    resp.ContentLength,
		ModTime: *resp.LastModified,
	}, nil
}

// statList list objects, if existed, it is directory
func (d *driver) statList(ctx context.Context, path string) (*storagedriver.FileInfoFields, error) {
	resp, err := d.oss.ListObjectsV2(ctx, &oss.ListObjectsV2Request{
		Bucket:  d.bucketPtr(),
		Prefix:  d.pathToKeyPtr(path),
		MaxKeys: 1,
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Contents) == 1 {
		if *resp.Contents[0].Key != d.pathToKey(path) {
			return &storagedriver.FileInfoFields{
				Path:  path,
				IsDir: true,
			}, nil
		}
		return &storagedriver.FileInfoFields{
			Path:    path,
			Size:    resp.Contents[0].Size,
			ModTime: *resp.Contents[0].LastModified,
		}, nil
	}
	if len(resp.CommonPrefixes) == 1 {
		return &storagedriver.FileInfoFields{
			Path:  path,
			IsDir: true,
		}, nil
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	fi, err := d.statHead(ctx, path)
	if err != nil || path == "/" { // is directory
		var se *oss.ServiceError
		if errors.As(err, &se) {
			fi, err := d.statList(ctx, path)
			if err != nil {
				return nil, err
			}
			return storagedriver.FileInfoInternal{FileInfoFields: *fi}, nil
		}
		return nil, err
	}
	return storagedriver.FileInfoInternal{FileInfoFields: *fi}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
// return only to the root level
func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	key := d.folderPathToKey(path)

	req := &oss.ListObjectsV2Request{
		Bucket:    d.bucketPtr(),
		Prefix:    oss.Ptr(key),
		Delimiter: oss.Ptr("/"),
		MaxKeys:   1000,
	}
	var files []string
	var directories []string

	isTruncated := true
	for isTruncated {
		v, err := d.oss.ListObjectsV2(ctx, req)
		if err != nil {
			return nil, parseNotFoundError(path, err)
		}
		for _, content := range v.Contents {
			if *content.Key != key {
				files = append(files, d.keyToPath(*content.Key))
			}
		}
		for _, commonPrefix := range v.CommonPrefixes {
			directories = append(directories, d.keyToPath(*commonPrefix.Prefix))
		}
		isTruncated = v.IsTruncated
		req.ContinuationToken = v.NextContinuationToken
	}
	// all the files include directories
	filePaths := append(files, directories...)
	if path != "/" && len(filePaths) == 0 {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	return filePaths, nil
}

func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	copier := d.oss.NewCopier()

	copyReq := &oss.CopyObjectRequest{
		Bucket:          d.bucketPtr(),
		Key:             d.pathToKeyPtr(destPath),
		SourceBucket:    d.bucketPtr(),
		SourceKey:       d.pathToKeyPtr(sourcePath),
		ForbidOverwrite: oss.Ptr("false"),
	}

	// Copy
	if _, err := copier.Copy(ctx, copyReq); err != nil {
		var se *oss.ServiceError
		if errors.As(err, &se) && se.StatusCode == http.StatusNotFound {
			return storagedriver.PathNotFoundError{Path: sourcePath, DriverName: d.Name()}
		}
		return err
	}

	// Delete the source file after copying
	if err := d.Delete(ctx, sourcePath); err != nil {
		return err
	}

	return nil
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
// We must be careful since SDK does not guarantee read after delete consistency
func (d *driver) Delete(ctx context.Context, path string) error {
	ossObjects := make([]oss.DeleteObject, 0, listMax)
	ossKey := d.pathToKey(path)
	listObjectsReq := &oss.ListObjectsV2Request{
		Bucket: d.bucketPtr(),
		Prefix: d.pathToKeyPtr(path),
	}

	for {
		// list all the objects
		resp, err := d.oss.ListObjectsV2(ctx, listObjectsReq)

		// resp.Contents can only be empty on the first call
		// if there were no more results to return after the first call, resp.IsTruncated would have been false
		// and the loop would exit without recalling ListObjects
		if err != nil || len(resp.Contents) == 0 {
			return storagedriver.PathNotFoundError{Path: path}
		}

		for _, obj := range resp.Contents {
			// Skip if we encounter a obj that is not a subpath (so that deleting "/a" does not delete "/ab").
			if len(*obj.Key) > len(ossKey) && (*obj.Key)[len(ossKey)] != '/' {
				continue
			}
			ossObjects = append(ossObjects, oss.DeleteObject{
				Key: obj.Key,
			})
		}

		// Delete objects only if the list is not empty
		if len(ossObjects) > 0 {
			resp, err := d.oss.DeleteMultipleObjects(ctx, &oss.DeleteMultipleObjectsRequest{
				Bucket:  d.bucketPtr(),
				Objects: ossObjects,
			})
			if err != nil {
				return err
			}

			if len(resp.DeletedObjects) != len(ossObjects) {
				errs := make([]error, 0, len(ossObjects)-len(resp.DeletedObjects))
				deletedMap := make(map[string]bool)
				for _, deleted := range resp.DeletedObjects {
					deletedMap[*deleted.Key] = true
				}

				for _, obj := range ossObjects {
					if !deletedMap[*obj.Key] {
						errs = append(errs, errors.New(fmt.Sprintf("NOT delete %s", *obj.Key)))
					}
				}

				return storagedriver.Errors{
					DriverName: driverName,
					Errs:       errs,
				}
			}
		}
		ossObjects = ossObjects[:0]

		// resp.Contents must have at least one element, or we would have returned not found
		listObjectsReq.StartAfter = resp.Contents[len(resp.Contents)-1].Key

		if !resp.IsTruncated {
			break
		}
	}

	return nil
}

func (d *driver) RedirectURL(r *http.Request, path string) (string, error) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return "", nil
	}

	expireOption := func(po *oss.PresignOptions) {
		po.Expires = 20 * time.Minute
	}

	var presign *oss.PresignResult
	var err error
	switch r.Method {
	case http.MethodGet:
		presign, err = d.oss.Presign(context.Background(), &oss.GetObjectRequest{
			Bucket: d.bucketPtr(),
			Key:    d.pathToKeyPtr(path),
		}, expireOption)
	case http.MethodHead:
		presign, err = d.oss.Presign(context.Background(), &oss.HeadObjectRequest{
			Bucket: d.bucketPtr(),
			Key:    d.pathToKeyPtr(path),
		}, expireOption)
	default:
		return "", nil
	}

	if err != nil {
		return "", err
	}

	return presign.URL, nil
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (d *driver) Walk(ctx context.Context, from string, f storagedriver.WalkFn, options ...func(*storagedriver.WalkOptions)) error {
	walkOptions := &storagedriver.WalkOptions{}
	for _, o := range options {
		o(walkOptions)
	}

	var objectCount int64
	if err := d.doWalk(ctx, &objectCount, from, walkOptions.StartAfterHint, f); err != nil {
		return err
	}

	return nil
}

func (d *driver) doWalk(parentCtx context.Context, objectCount *int64, from, startAfter string, f storagedriver.WalkFn) error {
	var retError error
	// the most recent skip directory to avoid walking over undesirable files
	var prevSkipDir string

	req := &oss.ListObjectsV2Request{
		Bucket:     d.bucketPtr(),
		Prefix:     d.pathToKeyPtr(from),
		StartAfter: d.pathToKeyPtr(startAfter),
		MaxKeys:    1000, // max 1000
	}
	ctx, done := dcontext.WithTrace(parentCtx)
	defer done("oss.ListObjectV2Request(%s)", req)
	isTruncated := true
	for isTruncated {
		// list all the objects
		res, err := d.oss.ListObjectsV2(ctx, req)
		if err != nil {
			return err
		}
		walkInfos := make([]storagedriver.FileInfoInternal, 0, len(res.Contents))
		for _, content := range res.Contents {
			if strings.HasSuffix(*content.Key, "/") { // directory
				walkInfos = append(walkInfos, storagedriver.FileInfoInternal{
					FileInfoFields: storagedriver.FileInfoFields{
						IsDir: true,
						Path:  d.folderKeyToPath(*content.Key),
					},
				})
			} else { // file object
				// last modification time
				walkInfos = append(walkInfos, storagedriver.FileInfoInternal{
					FileInfoFields: storagedriver.FileInfoFields{
						IsDir:   false,
						Size:    content.Size,
						ModTime: *content.LastModified,
						Path:    d.keyToPath(*content.Key),
					},
				})
			}
		}
		isTruncated = res.IsTruncated
		req.ContinuationToken = res.NextContinuationToken
		// iterative
		for _, walkInfo := range walkInfos {
			// skip any results under the last skip directory
			if prevSkipDir != "" && strings.HasPrefix(walkInfo.Path(), prevSkipDir) {
				continue
			}

			err := f(walkInfo)
			*objectCount++

			if err != nil {
				if errors.Is(err, storagedriver.ErrSkipDir) {
					prevSkipDir = walkInfo.Path()
					continue
				}
				if errors.Is(err, storagedriver.ErrFilledBuffer) {
					isTruncated = false
					break
				}
				retError = err
				isTruncated = false
				break
			}
		}
	}
	if retError != nil {
		return retError
	}
	return nil
}

var _ storagedriver.FileWriter = &writer{}

// writer uploads parts to OSS in a buffered stream data where the length of each part
// is [writer.driver.ChunkSize].
type writer struct {
	ctx       context.Context
	driver    *driver
	key       string
	uploadID  string     // multipart upload id
	parts     []oss.Part // all chunks record
	size      int64
	buf       *bytes.Buffer
	closed    bool
	committed bool
	cancelled bool
}

func (d *driver) newWriter(ctx context.Context, key, uploadID string, parts []oss.Part) storagedriver.FileWriter {
	var size int64
	for _, part := range parts {
		size += part.Size
	}
	return &writer{
		ctx:      ctx,
		driver:   d,
		key:      key,
		uploadID: uploadID,
		parts:    parts,
		size:     size,
		buf:      d.pool.Get().(*bytes.Buffer),
	}
}

func convertUploadParts(parts []oss.Part) []oss.UploadPart {
	var uploadParts []oss.UploadPart
	for _, part := range parts {
		uploadParts = append(uploadParts, oss.UploadPart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		})
	}
	return uploadParts
}

// Write writes data to the buffer and uploads it as a part if the buffer exceeds the chunk size.
func (w *writer) Write(p []byte) (int, error) {
	if err := w.done(); err != nil {
		return 0, err
	}
	if len(w.parts) > 0 && int(w.parts[len(w.parts)-1].Size) < minChunkSize {
		// complete the uploads
		sort.Slice(w.parts, func(i, j int) bool {
			return w.parts[i].PartNumber < w.parts[j].PartNumber
		})

		_, err := w.driver.oss.CompleteMultipartUpload(w.ctx, &oss.CompleteMultipartUploadRequest{
			Bucket:   w.driver.bucketPtr(),
			Key:      oss.Ptr(w.key),
			UploadId: oss.Ptr(w.uploadID),
			CompleteMultipartUpload: &oss.CompleteMultipartUpload{
				Parts: convertUploadParts(w.parts),
			},
		})
		if err != nil {
			if err1 := w.Cancel(w.ctx); err1 != nil {
				return 0, errors.Join(err, err1)
			}
			return 0, err
		}
		// new
		res, err := w.driver.oss.InitiateMultipartUpload(w.ctx, &oss.InitiateMultipartUploadRequest{
			Bucket: w.driver.bucketPtr(),
			Key:    oss.Ptr(w.key),
		})
		if err != nil {
			return 0, err
		}
		w.uploadID = *res.UploadId
		// If the entire written file is smaller than minChunkSize, we need to make a new part from scratch
		if w.size < minChunkSize {
			res, err := w.driver.oss.GetObject(w.ctx, &oss.GetObjectRequest{
				Bucket: w.driver.bucketPtr(),
				Key:    oss.Ptr(w.key),
			})
			if err != nil {
				return 0, err
			}
			defer res.Body.Close()
			w.reset()
			if _, err := io.Copy(w.buf, res.Body); err != nil {
				return 0, err
			}
		}
	}

	n, _ := w.buf.Write(p)

	for w.buf.Len() >= int(w.driver.chunkSize) {
		if err := w.flush(); err != nil {
			return 0, fmt.Errorf("flush: %w", err)
		}
	}
	return n, nil
}

// Close flushes any remaining data in the buffer and releases the buffer back to the pool.
func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}
	w.closed = true
	defer w.releaseBuffer()

	return w.flush()
}

func (w *writer) reset() {
	w.buf.Reset()
	w.parts = nil
	w.size = 0
}

// releaseBuffer resets the buffer and returns it to the pool.
func (w *writer) releaseBuffer() {
	w.buf.Reset()
	w.driver.pool.Put(w.buf)
}

// Size returns the number of bytes written to this FileWriter.
func (w *writer) Size() int64 {
	return w.size
}

// Cancel aborts the multipart upload and closes the writer.
func (w *writer) Cancel(ctx context.Context) error {
	if err := w.done(); err != nil {
		return err
	}
	w.cancelled = true
	_, err := w.driver.oss.AbortMultipartUpload(ctx, &oss.AbortMultipartUploadRequest{
		Bucket:   w.driver.bucketPtr(),
		Key:      oss.Ptr(w.key),
		UploadId: oss.Ptr(w.uploadID),
	})
	return err
}

// Commit flushes any remaining data in the buffer and completes the multipart upload.
func (w *writer) Commit(ctx context.Context) error {
	if err := w.done(); err != nil {
		return err
	}
	// make sure the buffer is empty
	if err := w.flush(); err != nil {
		return err
	}
	w.committed = true
	if len(w.parts) == 0 {
		res, err := w.driver.oss.UploadPart(ctx, &oss.UploadPartRequest{
			Bucket:        w.driver.bucketPtr(),
			Key:           oss.Ptr(w.key),
			UploadId:      oss.Ptr(w.uploadID),
			PartNumber:    1,
			Body:          bytes.NewReader(nil),
			ContentLength: oss.Ptr(int64(0)),
		})
		if err != nil {
			return err
		}
		w.parts = append(w.parts, oss.Part{
			ETag:       res.ETag,
			PartNumber: 1,
		})
	}
	// sort by asc
	sort.Slice(w.parts, func(i, j int) bool {
		return w.parts[i].PartNumber < w.parts[j].PartNumber
	})

	_, err := w.driver.oss.CompleteMultipartUpload(ctx, &oss.CompleteMultipartUploadRequest{
		Bucket:   w.driver.bucketPtr(),
		Key:      oss.Ptr(w.key),
		UploadId: oss.Ptr(w.uploadID),
		CompleteMultipartUpload: &oss.CompleteMultipartUpload{
			Parts: convertUploadParts(w.parts),
		},
	})
	if err != nil {
		if err1 := w.Cancel(ctx); err1 != nil {
			return errors.Join(err, err1)
		}
		return err
	}
	return nil
}

// flush writes at most [w.driver.ChunkSize] of the buffer to OSS.
// The number of blocks supported is 1 to 10000, and the block size is 100KB to 5 GB
func (w *writer) flush() error {
	// buffer is empty
	if w.buf.Len() == 0 {
		return nil
	}
	r := bytes.NewReader(w.buf.Next(int(w.driver.chunkSize)))
	partSize := int64(r.Len())
	partNumber := len(w.parts) + 1

	res, err := w.driver.oss.UploadPart(w.ctx, &oss.UploadPartRequest{
		Bucket:        w.driver.bucketPtr(),
		Key:           oss.Ptr(w.key),
		UploadId:      oss.Ptr(w.uploadID),
		PartNumber:    int32(partNumber),
		Body:          r,
		ContentLength: oss.Ptr(partSize),
	})
	if err != nil {
		return fmt.Errorf("upload part: %w", err)
	}
	w.parts = append(w.parts, oss.Part{
		ETag:       res.ETag,
		PartNumber: int32(partNumber),
		Size:       partSize,
	})
	w.size += partSize
	return nil
}

// done returns an error if the writer is in an invalid state.
func (w *writer) done() error {
	switch {
	case w.closed:
		return fmt.Errorf("already closed")
	case w.committed:
		return fmt.Errorf("already committed")
	case w.cancelled:
		return fmt.Errorf("already cancelled")
	}
	return nil
}
