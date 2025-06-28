package oss

import (
	"errors"

	"github.com/mitchellh/mapstructure"
)

type Parameters struct {
	AccessKeyID     string `mapstructure:"access_key_id"`
	AccessKeySecret string `mapstructure:"access_key_secret"`
	Region          string `mapstructure:"region"`
	Bucket          string `mapstructure:"bucket"`
	RootDirectory   string `mapstructure:"root_directory"`
	ChunkSize       int64  `mapstructure:"chunk_size"`
}

func NewParameters(parameters map[string]interface{}) (*Parameters, error) {
	const defaultChunkSize = 5 * 1024 * 1024
	params := Parameters{ChunkSize: defaultChunkSize}

	if err := mapstructure.Decode(parameters, &params); err != nil {
		return nil, err
	}
	if params.AccessKeyID == "" {
		return nil, errors.New("access_key_id is required")
	}
	if params.AccessKeySecret == "" {
		return nil, errors.New("access_key_secret is required")
	}
	if params.Region == "" {
		return nil, errors.New("region is required")
	}
	if params.Bucket == "" {
		return nil, errors.New("bucket is required")
	}
	if params.ChunkSize < 100*1024 { // min 100KB chunk size
		return nil, errors.New("chunk_size must be at least 100KB")
	}
	return &params, nil
}
