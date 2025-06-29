package oss

import (
	"context"
	"net/http"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
)

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
	getReq := &oss.GetObjectRequest{
		Bucket: d.ossBucketPtr(),
		Key:    d.ossKeyPtr(path),
	}

	expireOption := func(po *oss.PresignOptions) {
		po.Expires = 15 * time.Minute
	}

	presignResult, err := d.client.Presign(context.Background(), getReq, expireOption)
	if err != nil {
		return "", err
	}

	return presignResult.URL, nil
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

func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	//TODO implement me
	panic("implement me")
}
