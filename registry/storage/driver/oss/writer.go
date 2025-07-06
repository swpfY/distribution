package oss

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
)

const (
	maxChunkSize = 5 << 30 // 5GB
)

type multipartSession struct {
	uploadID string
	partNum  int32 // the latest part number
	path     string
	size     int64
	parts    []oss.UploadPart
	lock     sync.Mutex
}

var _ storagedriver.FileWriter = &writer{}

type writer struct {
	ctx       context.Context
	driver    *driver
	session   *multipartSession
	buffer    *bytes.Buffer
	partNum   int32 // the part number for that writer
	closed    bool
	committed bool
	cancelled bool
}

// Writer returns a FileWriter for the specified path
func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	if !append {
		_ = d.Delete(ctx, path)
	}
	writer := d.newWriter(ctx, path)
	if writer == nil {
		return nil, fmt.Errorf("failed to create new writer for path: %s", path)
	}

	return writer, nil
}

// newWriter initializes a new multipart upload session or retrieves an existing one
func (d *driver) newWriter(ctx context.Context, path string) storagedriver.FileWriter {
	// find or create a new multipart session
	sessionInterface, loaded := d.sessions.LoadOrStore(path, &multipartSession{
		lock: sync.Mutex{},
	})

	session := sessionInterface.(*multipartSession)
	// Lock, init session if not loaded
	session.lock.Lock()
	defer session.lock.Unlock()

	if !loaded && session.uploadID == "" {
		// init new session
		newSession, err := d.newSession(ctx, path)
		if err != nil {
			log.Fatalf("failed to create new multipart session: %v", err)
		}
		session.uploadID = newSession.uploadID
		session.path = newSession.path
		session.partNum = newSession.partNum
		session.size = newSession.size
	}

	// create a new writer
	session.partNum++
	return &writer{
		ctx:       ctx,
		driver:    d,
		session:   session,
		buffer:    bytes.NewBuffer(nil),
		partNum:   session.partNum,
		closed:    false,
		committed: false,
		cancelled: false,
	}
}

// newSession creates a new multipart upload session for the given path
func (d *driver) newSession(ctx context.Context, path string) (*multipartSession, error) {
	req := &oss.InitiateMultipartUploadRequest{
		Bucket: d.ossBucketPtr(),
		Key:    d.ossKeyPtr(path),
	}

	resp, err := d.client.InitiateMultipartUpload(ctx, req)
	if err != nil {
		log.Fatalf("failed to initiate multipart upload: %v", err)
	}
	uploadId := *resp.UploadId

	return &multipartSession{
		uploadID: uploadId,
		partNum:  0,
		path:     path,
		size:     0,
		lock:     sync.Mutex{},
	}, nil
}

// Write writes data to the buffer and manages the multipart upload process
func (w *writer) Write(p []byte) (n int, err error) {
	if err := w.checkDone(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil // no data to write
	}

	if w.buffer.Len()+len(p) > maxChunkSize {
		if err := w.flush(w.ctx); err != nil {
			return 0, fmt.Errorf("failed to reload writer: %w", err)
		}
	}

	w.session.lock.Lock()
	defer w.session.lock.Unlock()

	n, err = w.buffer.Write(p)
	if err != nil {
		return n, fmt.Errorf("failed to write data to buffer: %w", err)
	}

	w.session.size += int64(n)

	return n, nil
}

// flush uploads the current buffer to OSS as a part of the multipart upload
// and resets the buffer & partNum for the next write operation
func (w *writer) flush(ctx context.Context) error {
	if err := w.checkDone(); err != nil {
		return err
	}

	if w.buffer.Len() == 0 {
		return nil // nothing to upload
	}

	req := &oss.UploadPartRequest{
		Bucket:     w.driver.ossBucketPtr(),
		Key:        w.driver.ossKeyPtr(w.session.path),
		UploadId:   oss.Ptr(w.session.uploadID),
		PartNumber: w.partNum,
		Body:       bytes.NewReader(w.buffer.Bytes()),
	}

	resp, err := w.driver.client.UploadPart(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to upload part: %w", err)
	}

	w.session.lock.Lock()
	defer w.session.lock.Unlock()

	// Update session state
	w.session.size += int64(len(w.buffer.Bytes()))
	w.session.parts = append(w.session.parts, oss.UploadPart{
		PartNumber: w.partNum,
		ETag:       resp.ETag,
	})

	w.buffer.Reset()
	w.session.partNum++
	w.partNum = w.session.partNum

	return nil
}

// Size returns the total size of the data written
// including the size of the current buffer and the already uploaded parts
// BUT NOT considering the buffers that other writers hold
func (w *writer) Size() int64 {
	w.session.lock.Lock()
	defer w.session.lock.Unlock()

	return w.session.size + int64(w.buffer.Len())
}

func (w *writer) Commit(ctx context.Context) error {
	if err := w.checkDone(); err != nil {
		return err
	}

	if w.buffer.Len() > 0 {
		if err := w.flush(ctx); err != nil {
			return fmt.Errorf("failed to flush buffer before commit: %w", err)
		}
	}

	w.committed = true

	req := &oss.CompleteMultipartUploadRequest{
		Bucket:   w.driver.ossBucketPtr(),
		Key:      w.driver.ossKeyPtr(w.session.path),
		UploadId: oss.Ptr(w.session.uploadID),
		CompleteMultipartUpload: &oss.CompleteMultipartUpload{
			Parts: w.session.parts,
		},
	}
	_, err := w.driver.client.CompleteMultipartUpload(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}
	// Remove the session from the map
	w.driver.sessions.Delete(w.session.path)
	return nil
}

func (w *writer) Cancel(ctx context.Context) error {
	if err := w.checkDone(); err != nil {
		return err
	}

	w.cancelled = true
	_, err := w.driver.client.AbortMultipartUpload(ctx, &oss.AbortMultipartUploadRequest{
		Bucket:   w.driver.ossBucketPtr(),
		Key:      w.driver.ossKeyPtr(w.session.path),
		UploadId: oss.Ptr(w.session.uploadID),
	})

	if err != nil {
		return fmt.Errorf("failed to cancel multipart upload: %w", err)
	}

	// Remove the session from the map
	w.driver.sessions.Delete(w.session.path)
	return nil
}

func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}

	if w.buffer.Len() > 0 {
		if err := w.flush(w.ctx); err != nil {
			return fmt.Errorf("failed to flush buffer before closing: %w", err)
		}
	}

	w.closed = true
	return nil
}

func (w *writer) checkDone() error {
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
