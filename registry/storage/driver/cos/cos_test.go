package cos

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"
	"github.com/joho/godotenv"
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

	now := time.Now()
	timestamp := now.Format("20060102_150405")
	rootDirectory = fmt.Sprint("test-", timestamp, "/")

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

func TestPutAndGetContent(t *testing.T) {
	skipCheck(t)

	driver, err := cosDriverConstructor()
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}

	ctx := context.Background()
	path := "/main/file1.txt"
	content := []byte("hello oss")

	// 写入
	err = driver.PutContent(ctx, path, content)
	if err != nil {
		t.Fatalf("PutContent failed: %v", err)
	}

	// 读取
	read, err := driver.GetContent(ctx, path)
	if err != nil {
		t.Fatalf("GetContent failed: %v", err)
	}

	if string(read) != string(content) {
		t.Fatalf("content mismatch: expected %s, got %s", content, read)
	}
}

func TestStatAndList(t *testing.T) {
	skipCheck(t)

	driver, err := cosDriverConstructor()
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}

	ctx := context.Background()

	// 测试 Stat：文件存在
	filePath := "/main/stat_test.txt"
	content := []byte("stat content")
	if err := driver.PutContent(ctx, filePath, content); err != nil {
		t.Fatalf("PutContent failed: %v", err)
	}
	fi, err := driver.Stat(ctx, filePath)
	if err != nil {
		t.Fatalf("Stat failed for existing file: %v", err)
	}
	if fi.Size() != int64(len(content)) {
		t.Errorf("Stat size mismatch: expected %d, got %d", len(content), fi.Size())
	}
	if fi.Path() != filePath {
		t.Errorf("Stat path mismatch: expected %q, got %q", filePath, fi.Path())
	}
	if fi.IsDir() {
		t.Errorf("Stat IsDir should be false for file")
	}

	// 测试 Stat：文件不存在
	missing := "/main/no_such_file.txt"
	if _, err := driver.Stat(ctx, missing); err == nil {
		t.Errorf("Stat should have failed for missing file")
	} else if _, ok := err.(storagedriver.PathNotFoundError); !ok {
		t.Errorf("Stat error for missing file must be PathNotFoundError, got %T", err)
	}

	// 测试 List：在同一目录下创建多个文件
	dir := "/main/list_test"
	paths := []string{
		dir + "/a.txt",
		dir + "/b.txt",
	}
	for _, p := range paths {
		if err := driver.PutContent(ctx, p, []byte("x")); err != nil {
			t.Fatalf("PutContent failed for %s: %v", p, err)
		}
	}

	// 列出 dir
	entries, err := driver.List(ctx, dir)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	// 打印 entries
	t.Logf("List entries in %s: %v", dir, entries)
	// 结果可能无序，转为 map 方便判断
	m := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		m[e] = struct{}{}
	}
	for _, expected := range paths {
		if _, found := m[expected]; !found {
			t.Errorf("List missing entry %q in %v", expected, entries)
		}
	}
}

func TestReader(t *testing.T) {
	skipCheck(t)

	driver, err := cosDriverConstructor()
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}

	ctx := context.Background()
	path := "/main/file_reader.txt"
	content := []byte("hello oss reader test")

	// 写入文件
	err = driver.PutContent(ctx, path, content)
	if err != nil {
		t.Fatalf("PutContent failed: %v", err)
	}

	// 读取全量, offset = 0
	reader, err := driver.Reader(ctx, path, 0)
	if err != nil {
		t.Fatalf("Reader failed: %v", err)
	}
	defer reader.Close()

	readAll, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Reader ReadAll failed: %v", err)
	}

	if string(readAll) != string(content) {
		t.Fatalf("Reader content mismatch: expected %s, got %s", content, readAll)
	}

	// 读取部分, offset = 6
	offset := int64(6)
	reader2, err := driver.Reader(ctx, path, offset)
	if err != nil {
		t.Fatalf("Reader with offset failed: %v", err)
	}
	defer func(reader2 io.ReadCloser) {
		_ = reader2.Close()
	}(reader2)

	readPartial, err := io.ReadAll(reader2)
	if err != nil {
		t.Fatalf("Reader ReadAll with offset failed: %v", err)
	}

	expectedPartial := content[offset:]
	if string(readPartial) != string(expectedPartial) {
		t.Fatalf("Reader offset content mismatch: expected %s, got %s", expectedPartial, readPartial)
	}
}

func TestDelete(t *testing.T) {
	skipCheck(t)

	driver, err := cosDriverConstructor()
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}

	ctx := context.Background()
	path := "/main/to_delete.txt"
	content := []byte("to be deleted")

	// 先写入
	if err := driver.PutContent(ctx, path, content); err != nil {
		t.Fatalf("PutContent failed: %v", err)
	}

	// 确认存在
	if _, err := driver.Stat(ctx, path); err != nil {
		t.Fatalf("Stat before delete failed: %v", err)
	}

	// 调用 Delete
	if err := driver.Delete(ctx, path); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 再次 Stat 应报 PathNotFoundError
	if _, err := driver.Stat(ctx, path); err == nil {
		t.Errorf("Stat after delete should have failed")
	} else if _, ok := err.(storagedriver.PathNotFoundError); !ok {
		t.Errorf("Stat after delete error must be PathNotFoundError, got %T", err)
	}
}

func TestMove(t *testing.T) {
	skipCheck(t)

	driver, err := cosDriverConstructor()
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}

	ctx := context.Background()
	src := "/main/move_src.txt"
	dst := "/main/move_dst.txt"
	content := []byte("move me")

	// 写入源文件
	if err := driver.PutContent(ctx, src, content); err != nil {
		t.Fatalf("PutContent failed: %v", err)
	}

	// 调用 Move
	if err := driver.Move(ctx, src, dst); err != nil {
		t.Fatalf("Move failed: %v", err)
	}

	// 源文件应不存在
	if _, err := driver.Stat(ctx, src); err == nil {
		t.Errorf("Stat on src after move should have failed")
	} else if _, ok := err.(storagedriver.PathNotFoundError); !ok {
		t.Errorf("Stat on src after move error must be PathNotFoundError, got %T", err)
	}

	// 目标文件应存在且内容一致
	data, err := driver.GetContent(ctx, dst)
	if err != nil {
		t.Fatalf("GetContent on dst failed: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("Move content mismatch: expected %s, got %s", content, data)
	}
}
