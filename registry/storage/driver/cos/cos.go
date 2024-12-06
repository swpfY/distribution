package cos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/distribution/distribution/v3/internal/dcontext"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/base"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	driverName   = "cos"
	minChunkSize = 1024 * 1024
	chunkSize    = 10 * 1024 * 1024
	contentType  = "application/octet-stream"

	// multipartCopyThresholdSize defines the default object size
	// above which multipart copy will be used. (Copy Object - Copy is used
	// for objects at or below this size.)
	// Empirically, 32 MB is optimal. Reference from S3 driver
	multipartCopyThresholdSize = 32 * 1024 * 1024

	// default chunk size for multipart copy
	multipartCopyChunkSize = 32 * 1024 * 1024

	// defines the default maximum number of concurrent upload part
	multipartCopyMaxThread = 100
)

var _ storagedriver.StorageDriver = &driver{}

type driver struct {
	cosClient     *cos.Client
	pool          *sync.Pool // object buffer pool used to improve performance
	chunkSize     int        // multipart size
	rootDirectory string
	secretID      string
	secretKey     string
	bucketURL     string
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by
// TencentCloud COS Storage Service.
type Driver struct {
	baseEmbed
}

func init() {
	factory.Register(driverName, &cosDriverFactory{})
}

// the implementation of StorageDriverFactory interface
type cosDriverFactory struct{}

func (factory *cosDriverFactory) Create(ctx context.Context, parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	params, err := NewParameters(parameters)
	if err != nil {
		return nil, err
	}
	return New(ctx, params)
}

func New(ctx context.Context, params *Parameters) (*Driver, error) {
	// initialize cos client
	u, err := url.Parse(fmt.Sprintf("https://%s.cos.%s.myqcloud.com", params.Bucket, params.Region))
	if err != nil {
		return nil, errors.New("invalid bucket or region")
	}
	su, err := url.Parse(params.ServiceURL)
	if err != nil {
		return nil, errors.New("invalid service url")
	}
	b := &cos.BaseURL{
		BucketURL:  u,
		ServiceURL: su,
	}
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  params.SecretID,
			SecretKey: params.SecretKey,
		},
	})
	// driver object
	d := &driver{
		cosClient: client,
		pool: &sync.Pool{
			New: func() any { return &bytes.Buffer{} },
		},
		chunkSize:     chunkSize,
		secretID:      params.SecretID,
		secretKey:     params.SecretKey,
		rootDirectory: params.RootDirectory,
		bucketURL:     fmt.Sprintf("%s.cos.%s.myqcloud.com", params.Bucket, params.Region),
	}
	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
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

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, content []byte) error {
	opt := cos.ObjectPutOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
			ContentType: contentType,
		},
	}
	_, err := d.cosClient.Object.Put(ctx, d.pathToKey(path), bytes.NewReader(content), &opt)
	return err
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	option := cos.ObjectGetOptions{
		Range: fmt.Sprintf("bytes=%d-", offset),
	}
	resp, err := d.cosClient.Object.Get(ctx, d.pathToKey(path), &option)
	if err != nil {
		if resp != nil {
			// 416 Range Not Satisfiable
			if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
				return io.NopCloser(bytes.NewReader(nil)), nil
			}

			// 404 Not Found
			if resp.StatusCode == http.StatusNotFound {
				return nil, storagedriver.PathNotFoundError{Path: path}
			}
		}
		return nil, err
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
	key := d.pathToKey(path)

	// new multipart upload
	init := func() (storagedriver.FileWriter, error) {
		res, _, err := d.cosClient.Object.InitiateMultipartUpload(ctx, key, &cos.InitiateMultipartUploadOptions{
			ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
				ContentType: contentType,
			},
		})
		if err != nil {
			return nil, err
		}
		return d.newWriter(ctx, key, res.UploadID, nil), nil
	}

	// new data
	if !appendMode {
		return init()
	}

	opt := cos.ListMultipartUploadsOptions{
		Prefix: key, // target object
	}
	isTruncated := true
	for isTruncated {
		res, _, err := d.cosClient.Bucket.ListMultipartUploads(ctx, &opt)
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

		var allParts []cos.Object
		for _, multi := range res.Uploads {
			if key != multi.Key {
				continue
			}

			v, _, err := d.cosClient.Object.ListParts(ctx, key, multi.UploadID, nil)
			if err != nil {
				return nil, err
			}
			allParts = append(allParts, v.Parts...)
			for v.IsTruncated {
				v, _, err = d.cosClient.Object.ListParts(ctx, key, multi.UploadID, &cos.ObjectListPartsOptions{
					PartNumberMarker: v.NextPartNumberMarker,
				})
				if err != nil {
					return nil, err
				}
				allParts = append(allParts, v.Parts...)
			}
			return d.newWriter(ctx, key, multi.UploadID, allParts), nil
		}

		// next
		opt.KeyMarker = res.NextKeyMarker
		opt.UploadIDMarker = res.NextUploadIDMarker
		isTruncated = res.IsTruncated
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

// statList list objects, if existed, it is directory
func (d *driver) statList(ctx context.Context, path string) (*storagedriver.FileInfoFields, error) {
	key := d.pathToKey(path)
	res, _, err := d.cosClient.Bucket.Get(ctx, &cos.BucketGetOptions{
		Prefix:  key,
		MaxKeys: 1,
	})
	if err != nil {
		return nil, err
	}
	if len(res.Contents) == 1 {
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
	res, err := d.cosClient.Object.Head(ctx, d.pathToKey(path), nil)
	if err != nil || path == "/" { // is directory
		fi, err := d.statList(ctx, path)
		if err != nil {
			return nil, err
		}
		return storagedriver.FileInfoInternal{
			FileInfoFields: *fi,
		}, nil
	}
	// file info
	size, err := strconv.ParseInt(res.Header.Get("Content-Length"), 10, 64)
	modTime, err := time.Parse(time.RFC1123, res.Header.Get("Last-Modified"))
	if err != nil {
		return nil, err
	}

	return storagedriver.FileInfoInternal{
		FileInfoFields: storagedriver.FileInfoFields{
			Path:    path,
			IsDir:   false,
			Size:    size,
			ModTime: modTime,
		},
	}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
// return only to the root level
func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	key := d.pathToKey(path)
	if path != "/" {
		key += "/"
	}

	opt := &cos.BucketGetOptions{
		Prefix:    key,
		Delimiter: "/",
		MaxKeys:   1000,
		Marker:    "",
	}
	var files []string
	var directories []string

	isTruncated := true
	for isTruncated {
		v, _, err := d.cosClient.Bucket.Get(ctx, opt)
		if err != nil {
			return nil, err
		}
		for _, content := range v.Contents {
			if content.Key != key {
				files = append(files, d.keyToPath(content.Key))
			}
		}
		for _, commonPrefix := range v.CommonPrefixes {
			directories = append(directories, d.keyToPath(commonPrefix))
		}
		isTruncated = v.IsTruncated
		opt.Marker = v.NextMarker
	}
	// all the files include directories
	filePaths := append(files, directories...)
	if path != "/" && len(filePaths) == 0 {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	return filePaths, nil
}

// Move moves an object stored at sourcePath to destPath.
// Firstly it copy the object to target, then delete the source object
func (d *driver) Move(ctx context.Context, sourcePath, destPath string) error {
	if err := d.copy(ctx, sourcePath, destPath); err != nil {
		return err
	}
	return d.Delete(ctx, sourcePath)
}

// copy is used to copy single file.
func (d *driver) copy(ctx context.Context, sourcePath, destPath string) error {
	fileInfo, err := d.Stat(ctx, sourcePath)
	if err != nil {
		return err
	}
	// COS can copy objects up to 5GB in size with a Copy function.
	// For larger objects, the multipart upload API must be used.
	//
	// Empirically, multipart copy is fastest with 32 MB parts and is faster
	// than PUT Object - Copy for objects larger than 32 MB.
	key := d.pathToKey(destPath)
	sourceURL := fmt.Sprintf("%s/%s", d.bucketURL, d.pathToKey(sourcePath))

	if fileInfo.Size() <= multipartCopyThresholdSize {
		_, _, err := d.cosClient.Object.Copy(ctx, key, sourceURL, nil)
		if err != nil {
			return err
		}
		return nil
	}
	// file size is larger than 32MB
	v, _, err := d.cosClient.Object.InitiateMultipartUpload(ctx, key, &cos.InitiateMultipartUploadOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
			ContentType: contentType,
		},
	})

	// compute parts
	numParts := (fileInfo.Size() + multipartCopyChunkSize - 1) / multipartCopyChunkSize
	completedParts := make([]cos.Object, numParts)
	// chan is used to limit the maximum number of copy threads
	errChan := make(chan error, numParts)
	limiter := make(chan struct{}, multipartCopyMaxThread)

	for i := range completedParts {
		go func() {
			limiter <- struct{}{}
			// bytes range
			firstByte := i * multipartCopyChunkSize
			lastByte := int64(firstByte + multipartCopyChunkSize - 1)
			if lastByte >= fileInfo.Size() {
				lastByte = fileInfo.Size() - 1
			}
			// copy offset part
			res, _, err := d.cosClient.Object.CopyPart(ctx, key, v.UploadID, i+1, sourceURL, &cos.ObjectCopyPartOptions{
				XCosCopySourceRange: fmt.Sprintf("bytes=%d-%d", firstByte, lastByte),
			})
			if err == nil {
				completedParts[i] = cos.Object{
					ETag:       res.ETag,
					PartNumber: i + 1,
				}
			}

			errChan <- err
			<-limiter
		}()
	}
	// err catch
	for range completedParts {
		if err := <-errChan; err != nil {
			return err
		}
	}

	// sort parts by asc
	sort.Slice(completedParts, func(i, j int) bool {
		return completedParts[i].PartNumber < completedParts[j].PartNumber
	})

	//complete multipart upload
	_, _, err = d.cosClient.Object.CompleteMultipartUpload(ctx, key, v.UploadID, &cos.CompleteMultipartUploadOptions{
		Parts: completedParts,
	})

	return err
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
// It can delete single file or entire directory.
func (d *driver) Delete(ctx context.Context, path string) error {
	key := d.pathToKey(path)
	var marker string
	opt := &cos.BucketGetOptions{
		Prefix:  key,
		MaxKeys: 1000, // max 1000
	}
	isTruncated := true
	for isTruncated {
		opt.Marker = marker
		// list all the objects
		res, _, err := d.cosClient.Bucket.Get(ctx, opt)
		if err != nil || len(res.Contents) == 0 {
			return storagedriver.PathNotFoundError{Path: path}
		}

		// delete all the objects
		var objs []cos.Object
		for _, content := range res.Contents {
			if len(content.Key) > len(key) && content.Key[len(key)] != '/' {
				continue
			}
			objs = append(objs, cos.Object{Key: content.Key})
		}

		// delete multi objects
		if len(objs) > 0 {
			v, _, err := d.cosClient.Object.DeleteMulti(ctx, &cos.ObjectDeleteMultiOptions{
				Objects: objs,
			})
			if err != nil {
				return err
			}
			if len(v.Errors) > 0 {
				errs := make([]error, 0, len(v.Errors))
				for _, err := range v.Errors {
					errs = append(errs, errors.New(err.Message))
				}
				return storagedriver.Errors{
					DriverName: driverName,
					Errs:       errs,
				}
			}
		}

		isTruncated = res.IsTruncated
		marker = res.NextMarker
	}
	return nil
}

// RedirectURL returns a temporary URL which may be used to download the content
func (d *driver) RedirectURL(r *http.Request, path string) (string, error) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return "", nil
	}

	key := d.pathToKey(path)
	expired := 20 * time.Minute
	preSignedURL, err := d.cosClient.Object.GetPresignedURL(context.Background(), r.Method, key,
		d.secretID,
		d.secretKey,
		expired,
		nil,
	)
	if err != nil {
		return "", err
	}
	return preSignedURL.String(), nil
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn, options ...func(*storagedriver.WalkOptions)) error {
	walkOptions := &storagedriver.WalkOptions{}
	for _, o := range options {
		o(walkOptions)
	}

	var objectCount int64
	if err := d.doWalk(ctx, &objectCount, path, walkOptions.StartAfterHint, f); err != nil {
		return err
	}

	return nil
}

func (d *driver) doWalk(parentCtx context.Context, objectCount *int64, from, startAfter string, f storagedriver.WalkFn) error {
	var retError error
	// the most recent skip directory to avoid walking over undesirable files
	var prevSkipDir string

	key := d.pathToKey(from)
	opt := &cos.BucketGetOptions{
		Prefix:  key,
		MaxKeys: 1000, // max 1000
		Marker:  d.pathToKey(startAfter),
	}
	ctx, done := dcontext.WithTrace(parentCtx)
	defer done("cos.Bucket.Get(%s)", opt)
	isTruncated := true
	for isTruncated {
		// list all the objects
		res, _, err := d.cosClient.Bucket.Get(ctx, opt)
		if err != nil {
			return err
		}
		walkInfos := make([]storagedriver.FileInfoInternal, 0, len(res.Contents))
		for _, content := range res.Contents {
			if strings.HasSuffix(content.Key, "/") { // directory
				walkInfos = append(walkInfos, storagedriver.FileInfoInternal{
					FileInfoFields: storagedriver.FileInfoFields{
						IsDir: true,
						Path:  strings.TrimRight(content.Key, "/"),
					},
				})
			} else { // file object
				// last modification time
				modTime, err := time.Parse(time.RFC1123, content.LastModified)
				if err != nil {
					return err
				}
				walkInfos = append(walkInfos, storagedriver.FileInfoInternal{
					FileInfoFields: storagedriver.FileInfoFields{
						IsDir:   false,
						Size:    content.Size,
						ModTime: modTime,
						Path:    content.Key,
					},
				})
			}
		}
		isTruncated = res.IsTruncated
		opt.Marker = res.NextMarker
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

// add the prefix
func (d *driver) pathToKey(path string) string {
	return strings.TrimLeft(strings.TrimRight(d.rootDirectory, "/")+path, "/")
}

// remove the prefix
func (d *driver) keyToPath(key string) string {
	return "/" + strings.TrimRight(strings.TrimPrefix(key, d.rootDirectory), "/")
}

var _ storagedriver.FileWriter = &writer{}

// writer uploads parts to COS in a buffered stream data where the length of each part
// is [writer.driver.ChunkSize].
type writer struct {
	ctx       context.Context
	driver    *driver
	key       string
	uploadID  string       // multipart upload id
	parts     []cos.Object // all chunks record
	size      int64
	buf       *bytes.Buffer
	closed    bool
	committed bool
	cancelled bool
}

func (d *driver) newWriter(ctx context.Context, key, uploadID string, parts []cos.Object) storagedriver.FileWriter {
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

// multipart upload supports that the last part is less than 1MB in COS
// refer to: https://cloud.tencent.com/document/product/436/14112
func (w *writer) Write(p []byte) (int, error) {
	if err := w.done(); err != nil {
		return 0, err
	}
	if len(w.parts) > 0 && int(w.parts[len(w.parts)-1].Size) < minChunkSize {
		// complete the uploads
		sort.Slice(w.parts, func(i, j int) bool {
			return w.parts[i].PartNumber < w.parts[j].PartNumber
		})
		_, _, err := w.driver.cosClient.Object.CompleteMultipartUpload(w.ctx, w.key, w.uploadID,
			&cos.CompleteMultipartUploadOptions{
				Parts: w.parts,
			},
		)
		if err != nil {
			if err1 := w.Cancel(w.ctx); err1 != nil {
				return 0, errors.Join(err, err1)
			}
			return 0, err
		}
		// new
		res, _, err := w.driver.cosClient.Object.InitiateMultipartUpload(w.ctx, w.key, &cos.InitiateMultipartUploadOptions{
			ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
				ContentType: contentType,
			},
		})
		if err != nil {
			return 0, err
		}
		w.uploadID = res.UploadID
		// If the entire written file is smaller than minChunkSize, we need to make a new part from scratch
		if w.size < minChunkSize {
			res, err := w.driver.cosClient.Object.Get(w.ctx, w.key, nil)
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

	for w.buf.Len() >= w.driver.chunkSize {
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
	_, err := w.driver.cosClient.Object.AbortMultipartUpload(ctx, w.key, w.uploadID)
	return err
}

// Commit flushes any remaining data in the buffer and completes the multipart upload.
// calls cos.Object.CompleteMultipartUpload
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
		res, err := w.driver.cosClient.Object.UploadPart(
			ctx,
			w.key,
			w.uploadID,
			1,
			nil,
			&cos.ObjectUploadPartOptions{
				ContentLength: 0,
			},
		)
		if err != nil {
			return err
		}
		w.parts = append(w.parts, cos.Object{
			ETag:       res.Header.Get("ETag"),
			PartNumber: 1,
		})
	}
	// sort by asc
	sort.Slice(w.parts, func(i, j int) bool {
		return w.parts[i].PartNumber < w.parts[j].PartNumber
	})
	_, _, err := w.driver.cosClient.Object.CompleteMultipartUpload(ctx, w.key, w.uploadID,
		&cos.CompleteMultipartUploadOptions{
			Parts: w.parts,
		},
	)
	if err != nil {
		if err1 := w.Cancel(ctx); err1 != nil {
			return errors.Join(err, err1)
		}
		return err
	}
	return nil
}

// flush writes at most [w.driver.ChunkSize] of the buffer to COS.
// it calls cos.Object.UploadPart function that is used to upload part
// The number of blocks supported is 1 to 10000, and the block size is 1 MB to 5 GB
func (w *writer) flush() error {
	// buffer is empty
	if w.buf.Len() == 0 {
		return nil
	}
	r := bytes.NewReader(w.buf.Next(w.driver.chunkSize))
	partSize := int64(r.Len())
	partNumber := len(w.parts) + 1
	res, err := w.driver.cosClient.Object.UploadPart(w.ctx, w.key, w.uploadID,
		partNumber,
		r,
		&cos.ObjectUploadPartOptions{
			ContentLength: partSize,
		},
	)
	if err != nil {
		return fmt.Errorf("upload part: %w", err)
	}
	w.parts = append(w.parts, cos.Object{
		ETag:       res.Header.Get("ETag"),
		PartNumber: partNumber,
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
