package oss

import (
	"context"
	"net/http"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
)

func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	//TODO implement me
	panic("implement me")
}

func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	//TODO implement me
	panic("implement me")
}

func (d *driver) Delete(ctx context.Context, path string) error {
	//TODO implement me
	panic("implement me")
}

func (d *driver) RedirectURL(r *http.Request, path string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn, options ...func(*storagedriver.WalkOptions)) error {
	//TODO implement me
	panic("implement me")
}
