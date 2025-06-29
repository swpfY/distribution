package oss

import (
	"context"
	"io"
	"testing"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
)

func TestPutAndGetContent(t *testing.T) {
	skipCheck(t)

	driver, err := ossDriverConstructor()
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

	driver, err := ossDriverConstructor()
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

	driver, err := ossDriverConstructor()
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
