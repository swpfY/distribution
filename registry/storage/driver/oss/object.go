package oss

import (
	"context"
	"io"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
)

func (d driver) Name() string {
	//TODO implement me
	panic("implement me")
}

func (d driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (d driver) PutContent(ctx context.Context, path string, content []byte) error {
	//TODO implement me
	panic("implement me")
}

func (d driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	//TODO implement me
	panic("implement me")
}

func (d driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (d driver) List(ctx context.Context, path string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}
