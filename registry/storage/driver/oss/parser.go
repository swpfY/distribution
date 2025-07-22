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
}

func NewParameters(parameters map[string]interface{}) (*Parameters, error) {
	params := Parameters{}

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
	if params.RootDirectory != "" {
		params.RootDirectory = strings.Trim(params.RootDirectory, "/")
	}
	return &params, nil
}
