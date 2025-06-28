package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/base"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
)

func init() {
	factory.Register(driverName, &ossDriverFactory{})
}

const (
	driverName = "oss"
)

var _ storagedriver.StorageDriver = &driver{}

// driver is the core service for interacting with OSS
type driver struct {
	client        *oss.Client
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
		client:        client,
		bucket:        params.Bucket,
		chunkSize:     params.ChunkSize,
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
