package oss

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
)

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
