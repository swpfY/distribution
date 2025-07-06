package oss

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
)

// Name returns
func (d *driver) Name() string {
	return driverName
}

func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	req := &oss.GetObjectRequest{
		Bucket: d.ossBucketPtr(),
		Key:    d.ossKeyPtr(path),
	}

	resp, err := d.client.GetObject(ctx, req)
	if err != nil {
		return nil, err
	}

	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (d *driver) PutContent(ctx context.Context, path string, content []byte) error {
	req := &oss.PutObjectRequest{
		Bucket: d.ossBucketPtr(),
		Key:    d.ossKeyPtr(path),
		Body:   bytes.NewReader(content),
	}

	_, err := d.client.PutObject(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	req := &oss.GetObjectRequest{
		Bucket: d.ossBucketPtr(),
		Key:    d.ossKeyPtr(path),
		Range:  d.ossRange(offset),
	}

	resp, err := d.client.GetObject(ctx, req)
	if err != nil {
		var se *oss.ServiceError
		if errors.As(err, &se) {
			if se.StatusCode == http.StatusNotFound {
				return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
			}
			if se.StatusCode == http.StatusRequestedRangeNotSatisfiable {
				return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset, DriverName: d.Name()}
			}
		}
		return nil, err
	}

	// Return IO ReadCloser directly
	return resp.Body, nil
}

func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	req := &oss.HeadObjectRequest{
		Bucket: d.ossBucketPtr(),
		Key:    d.ossKeyPtr(path),
	}

	resp, err := d.client.HeadObject(ctx, req)
	if err != nil {
		var se *oss.ServiceError
		if errors.As(err, &se) && se.StatusCode == http.StatusNotFound {
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
		}
		return nil, err
	}
	// Head 请求无需手动 Close

	size := resp.ContentLength
	modTime := *resp.LastModified
	isDir := strings.HasSuffix(path, "/") // OSS treat directories as objects with a trailing slash

	return storagedriver.FileInfoInternal{
		FileInfoFields: storagedriver.FileInfoFields{
			Path:    path,
			Size:    size,
			ModTime: modTime,
			IsDir:   isDir,
		},
	}, nil
}

func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	p := d.OssGetPaginator(path)

	var keys []string

	for p.HasNext() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			key := d.keyToPath(oss.ToString(obj.Key))
			keys = append(keys, key)
		}
	}

	return keys, nil
}

func (d *driver) OssGetPaginator(path string) *oss.ListObjectsV2Paginator {
	folder := d.folderPathToKey(path)

	req := &oss.ListObjectsV2Request{
		Bucket: d.ossBucketPtr(),
		Prefix: oss.Ptr(folder),
	}

	return d.client.NewListObjectsV2Paginator(req)
}

func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	copier := d.client.NewCopier()

	copyReq := &oss.CopyObjectRequest{
		Bucket:          d.ossBucketPtr(),
		Key:             d.ossKeyPtr(destPath),
		SourceBucket:    d.ossBucketPtr(),
		SourceKey:       d.ossKeyPtr(sourcePath),
		ForbidOverwrite: oss.Ptr("false"),
	}

	// Copy
	if _, err := copier.Copy(ctx, copyReq); err != nil {
		return err
	}

	// Delete the source file after copying
	if err := d.Delete(ctx, sourcePath); err != nil {
		return err
	}

	return nil
}

func (d *driver) Delete(ctx context.Context, path string) error {
	req := &oss.DeleteObjectRequest{
		Bucket: d.ossBucketPtr(),
		Key:    d.ossKeyPtr(path),
	}

	if _, err := d.client.DeleteObject(ctx, req); err != nil {
		return err // OSS 在文件不存在时, 请求删除文件也返回 204, 因此直接忽略了文件不存在的情况
	}

	return nil
}

func (d *driver) RedirectURL(r *http.Request, path string) (string, error) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return "", nil
	}

	getReq := &oss.GetObjectRequest{
		Bucket: d.ossBucketPtr(),
		Key:    d.ossKeyPtr(path),
	}

	expireOption := func(po *oss.PresignOptions) {
		po.Expires = 15 * time.Minute
	}

	presign, err := d.client.Presign(context.Background(), getReq, expireOption)
	if err != nil {
		return "", err
	}

	return presign.URL, nil
}

func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn, options ...func(*storagedriver.WalkOptions)) error {
	p := d.OssGetPaginator(path)

	for p.HasNext() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return err
		}

		for _, obj := range page.Contents {
			filePath := d.keyToPath(oss.ToString(obj.Key))
			size := obj.Size

			fi := storagedriver.FileInfoInternal{
				FileInfoFields: storagedriver.FileInfoFields{
					Path:    filePath,
					Size:    size,
					ModTime: oss.ToTime(obj.LastModified),
					IsDir:   false,
				},
			}

			if err := f(fi); err != nil {
				return err // 回调出错立即终止
			}
		}
	}

	return nil
}
