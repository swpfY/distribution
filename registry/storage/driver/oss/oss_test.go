package oss

import (
	"context"
	"fmt"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"
	"github.com/joho/godotenv"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	envAccessKeyID     = "OSS_STORAGE_ACCESS_KEY_ID"
	envAccessKeySecret = "OSS_STORAGE_ACCESS_KEY_SECRET"
	envRegion          = "OSS_STORAGE_REGION"
	envBucket          = "OSS_STORAGE_BUCKET"
	envRootDirectory   = "OSS_STORAGE_ROOT_DIRECTORY"
)

var (
	ossDriverConstructor func() (storagedriver.StorageDriver, error)
	skipCheck            func(tb testing.TB)
	timestamp            string
)

func init() {
	_ = godotenv.Load()
	now := time.Now()
	timestamp = now.Format("20060102_150405")

	var (
		accessKeyID     = os.Getenv(envAccessKeyID)
		accessKeySecret = os.Getenv(envAccessKeySecret)
		region          = os.Getenv(envRegion)
		bucket          = os.Getenv(envBucket)
		rootDirectory   = os.Getenv(envRootDirectory)
	)
	if rootDirectory == "" {
		rootDirectory = fmt.Sprint("test-", timestamp)
	}

	var missing []string
	if accessKeyID == "" {
		missing = append(missing, envAccessKeyID)
	}
	if accessKeySecret == "" {
		missing = append(missing, envAccessKeySecret)
	}
	if region == "" {
		missing = append(missing, envRegion)
	}
	if bucket == "" {
		missing = append(missing, envBucket)
	}

	ossDriverConstructor = func() (storagedriver.StorageDriver, error) {
		parameters := map[string]interface{}{
			"accessid":      accessKeyID,
			"secret":        accessKeySecret,
			"region":        region,
			"bucket":        bucket,
			"rootdirectory": rootDirectory,
		}
		params, err := NewParameters(parameters)
		if err != nil {
			return nil, err
		}
		return New(context.Background(), params)
	}

	skipCheck = func(tb testing.TB) {
		tb.Helper()
		if len(missing) > 0 {
			tb.Skipf("Must set %s environment variables to run OSS tests", strings.Join(missing, ", "))
		}
	}
}

func TestOssDriverSuite(t *testing.T) {
	skipCheck(t)
	testsuites.Driver(t, ossDriverConstructor)
}

func BenchmarkOssDriverSuite(b *testing.B) {
	skipCheck(b)
	testsuites.BenchDriver(b, ossDriverConstructor)
}
