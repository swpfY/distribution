package cos

import (
	"context"
	"fmt"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"
	"github.com/joho/godotenv"
	"os"
	"strings"
	"testing"
)

const (
	envSecretID      = "COS_STORAGE_SECRET_ID"
	envSecretKey     = "COS_STORAGE_SECRET_KEY"
	envRegion        = "COS_STORAGE_REGION"
	envBucket        = "COS_STORAGE_BUCKET"
	envRootDirectory = "COS_STORAGE_ROOT_DIRECTORY"
)

var (
	cosDriverConstructor func() (storagedriver.StorageDriver, error)
	skipCheck            func(tb testing.TB)
)

func init() {
	// load env
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error loading .env file")
	}
	fmt.Println(os.Getenv("COS_STORAGE_SECRET_ID"))

	var (
		secretID      string
		secretKey     string
		region        string
		bucket        string
		serviceURL    = "https://service.cos.myqcloud.com"
		rootDirectory string
	)

	config := []struct {
		env       string
		value     *string
		missingOk bool
	}{
		{envSecretID, &secretID, false},
		{envSecretKey, &secretKey, false},
		{envRegion, &region, false},
		{envBucket, &bucket, false},
		{envRootDirectory, &rootDirectory, true},
	}

	var missing []string
	for _, v := range config {
		*v.value = os.Getenv(v.env)
		if *v.value == "" && !v.missingOk {
			missing = append(missing, v.env)
		}
	}

	cosDriverConstructor = func() (storagedriver.StorageDriver, error) {
		parameters := map[string]interface{}{
			"secretid":      secretID,
			"secretkey":     secretKey,
			"region":        region,
			"bucket":        bucket,
			"serviceurl":    serviceURL,
			"rootdirectory": rootDirectory,
		}
		params, err := NewParameters(parameters)
		if err != nil {
			return nil, err
		}
		return New(context.Background(), params)
	}

	// Skip COS storage driver tests if environment variable parameters are not provided
	skipCheck = func(tb testing.TB) {
		tb.Helper()
		if len(missing) > 0 {
			tb.Skipf("Must set %s environment variables to run COS tests", strings.Join(missing, ", "))
		}
	}
}

func TestCosDriverSuite(t *testing.T) {
	skipCheck(t)
	testsuites.Driver(t, cosDriverConstructor)
}

func BenchmarkCosDriverSuite(b *testing.B) {
	skipCheck(b)
	testsuites.BenchDriver(b, cosDriverConstructor)
}
