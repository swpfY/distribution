package oss

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestCleanupFlow(t *testing.T) {
	skipCheck(t)

	driver, err := ossDriverConstructor()
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}

	ctx := context.Background()

	// 测试路径（复杂多级）
	tempPath := "/test-cleanup/temp/file1"
	finalPath := "/test-cleanup/final/file1"

	content := []byte("hello cleanup test")

	// 1. 测试 Cancel 是否清理
	writer, err := driver.Writer(ctx, tempPath, false)
	if err != nil {
		t.Fatalf("Writer failed: %v", err)
	}

	if _, err := writer.Write(content); err != nil {
		t.Fatalf("Writer.Write failed: %v", err)
	}

	if err := writer.Cancel(ctx); err != nil {
		t.Fatalf("Writer.Cancel failed: %v", err)
	}

	// Cancel 后检查对象是否被删除
	_, err = driver.Stat(ctx, tempPath)
	if err == nil {
		t.Errorf("Stat should have failed after Cancel, object should be deleted: %s", tempPath)
	}

	// 2. 测试 Move 是否清理源对象
	if err := driver.PutContent(ctx, tempPath, content); err != nil {
		t.Fatalf("PutContent failed: %v", err)
	}

	if err := driver.Move(ctx, tempPath, finalPath); err != nil {
		t.Fatalf("Move failed: %v", err)
	}

	// Move 后源对象应不存在
	_, err = driver.Stat(ctx, tempPath)
	if err == nil {
		t.Errorf("Stat should have failed for source after Move, but object still exists: %s", tempPath)
	}

	// Move 后目标对象应存在
	_, err = driver.Stat(ctx, finalPath)
	if err != nil {
		t.Errorf("Stat should have succeeded for destination after Move: %s", finalPath)
	}

	// 3. 测试 Delete 是否删除复杂路径
	if err := driver.Delete(ctx, finalPath); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = driver.Stat(ctx, finalPath)
	if err == nil {
		t.Errorf("Stat should have failed after Delete, but object still exists: %s", finalPath)
	}

	// 4. Walk 检查是否存在任何残留对象
	remaining, err := driver.List(ctx, "/test-cleanup")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("Unexpected remaining files: %v", remaining)
	}
}

func TestConcurrentWriter(t *testing.T) {
	skipCheck(t)

	driver, err := ossDriverConstructor()
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}

	ctx := context.Background()
	path := "/main/concurrent_writer_test.txt"

	// 构造 5 个 goroutine，每个写入不同内容
	parts := [][]byte{
		[]byte("part1-"),
		[]byte("part2-"),
		[]byte("part3-"),
		[]byte("part4-"),
		[]byte("part5"),
	}

	writer, err := driver.Writer(ctx, path, false)
	if err != nil {
		t.Fatalf("Writer failed: %v", err)
	}

	var wg sync.WaitGroup
	for _, p := range parts {
		wg.Add(1)
		go func(data []byte) {
			defer wg.Done()
			for i := 0; i < 100; i++ { // 每个 goroutine 写入 100 次，制造强并发
				if _, err := writer.Write(data); err != nil {
					t.Errorf("Write failed: %v", err)
					return
				}
			}
		}(p)
	}

	wg.Wait()

	// 提交写入
	if err := writer.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// 读取文件
	content, err := driver.GetContent(ctx, path)
	if err != nil {
		t.Fatalf("GetContent failed: %v", err)
	}

	// 检查文件大小是否符合预期
	expectedSize := 0
	for _, p := range parts {
		expectedSize += len(p) * 100 // 每个部分写入 100 次
	}

	if len(content) != expectedSize {
		t.Fatalf("Size mismatch: expected %d bytes, got %d bytes", expectedSize, len(content))
	}

	t.Logf("Concurrent write test passed, final size: %d bytes", len(content))
}

func TestMultipleWritersConcurrent(t *testing.T) {
	skipCheck(t)

	driver, err := ossDriverConstructor()
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}

	ctx := context.Background()
	numWriters := 8
	var wg sync.WaitGroup

	writeOne := func(path string) {
		defer wg.Done()

		writer, err := driver.Writer(ctx, path, false)
		if err != nil {
			t.Errorf("Writer failed for %s: %v", path, err)
			return
		}

		content := []byte(strings.Repeat("a", 1024*1024)) // 1MB chunk

		for i := 0; i < 10; i++ { // 每个 Writer 写 10MB
			if _, err := writer.Write(content); err != nil {
				t.Errorf("Write failed for %s: %v", path, err)
				return
			}
		}

		if err := writer.Commit(ctx); err != nil {
			t.Errorf("Commit failed for %s: %v", path, err)
			return
		}
	}

	for i := 0; i < numWriters; i++ {
		path := fmt.Sprintf("/multi_writer_test/file_%d.txt", i)
		wg.Add(1)
		go writeOne(path)
	}

	wg.Wait()

	t.Log("Multi-writer concurrent test finished.")
}
