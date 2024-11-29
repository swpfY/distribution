package cos

import (
	"errors"
	"github.com/mitchellh/mapstructure"
)

type Parameters struct {
	SecretID      string `mapstructure:"secretid"`
	SecretKey     string `mapstructure:"secretkey"`
	Region        string `mapstructure:"region"`
	Bucket        string `mapstructure:"bucket"` // bucket name
	RootDirectory string `mapstructure:"rootdirectory"`
	ServiceURL    string `mapstructure:"serviceurl"`
}

const (
	serviceURL = "https://service.cos.myqcloud.com"
)

func NewParameters(parameters map[string]interface{}) (*Parameters, error) {
	params := Parameters{
		ServiceURL: serviceURL,
	}
	if err := mapstructure.Decode(parameters, &params); err != nil {
		return nil, err
	}
	if params.SecretID == "" {
		return nil, errors.New("secretid is required")
	}
	if params.SecretKey == "" {
		return nil, errors.New("secretkey is required")
	}
	if params.Region == "" {
		return nil, errors.New("no region parameter provided")
	}
	if params.Bucket == "" {
		return nil, errors.New("bucket id the bucket name")
	}
	if params.ServiceURL == "" {
		return nil, errors.New("serviceurl is default https://service.cos.myqcloud.com")
	}

	return &params, nil
}
