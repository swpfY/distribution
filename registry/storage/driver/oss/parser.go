package oss

import (
	"errors"
	"strings"

	"github.com/mitchellh/mapstructure"
)

type Parameters struct {
	AccessKeyID     string `mapstructure:"accessid"`
	AccessKeySecret string `mapstructure:"secret"`
	Region          string `mapstructure:"region"`
	Bucket          string `mapstructure:"bucket"`
	RootDirectory   string `mapstructure:"rootdirectory"`
	ChunkSize       int64  `mapstructure:"chunk"`
}

func NewParameters(parameters map[string]interface{}) (*Parameters, error) {
	const defaultChunkSize = 5 * 1024 * 1024
	params := Parameters{ChunkSize: defaultChunkSize}

	if err := mapstructure.Decode(parameters, &params); err != nil {
		return nil, err
	}
	if params.AccessKeyID == "" {
		return nil, errors.New("accessid is required")
	}
	if params.AccessKeySecret == "" {
		return nil, errors.New("secret is required")
	}
	if params.Region == "" {
		return nil, errors.New("region is required")
	}
	if params.Bucket == "" {
		return nil, errors.New("bucket is required")
	}
	if params.ChunkSize < 100<<10 { // min 100KB chunk size
		return nil, errors.New("chunk must be at least 100KB")
	}
	if params.RootDirectory != "" {
		params.RootDirectory = strings.Trim(params.RootDirectory, "/")
	}
	return &params, nil
}
