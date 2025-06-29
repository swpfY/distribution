package oss

import (
	"context"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"net/http"
	"strings"
	"testing"
)

func TestDelete(t *testing.T) {
	skipCheck(t)

	driver, err := ossDriverConstructor()
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

	driver, err := ossDriverConstructor()
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

func TestRedirectURL(t *testing.T) {
	skipCheck(t)

	driver, err := ossDriverConstructor()
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}

	ctx := context.Background()
	path := "/main/redirect.txt"
	content := []byte("redirect test")

	// 写入文件
	if err := driver.PutContent(ctx, path, content); err != nil {
		t.Fatalf("PutContent failed: %v", err)
	}

	// 构造假请求，仅用于方法签名
	fakeReq, _ := http.NewRequest("GET", "http://example.com", nil)
	url, err := driver.RedirectURL(fakeReq, path)
	if err != nil {
		t.Fatalf("RedirectURL failed: %v", err)
	}

	if !strings.HasPrefix(url, "https://") {
		t.Errorf("RedirectURL should return https URL, got %q", url)
	}
	if !strings.Contains(url, "redirect.txt") {
		t.Errorf("RedirectURL should contain the file name: %q", url)
	}

}

func TestWalk(t *testing.T) {
	skipCheck(t)

	driver, err := ossDriverConstructor()
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}

	ctx := context.Background()
	prefix := "/main/walk_test"
	paths := []string{
		prefix + "/a.txt",
		prefix + "/sub/b.txt",
		prefix + "/sub/c.txt",
	}
	for _, p := range paths {
		if err := driver.PutContent(ctx, p, []byte("x")); err != nil {
			t.Fatalf("PutContent failed for %s: %v", p, err)
		}
	}

	var walked []string
	walkFn := func(fi storagedriver.FileInfo) error {
		walked = append(walked, fi.Path())
		return nil
	}

	if err := driver.Walk(ctx, prefix, walkFn); err != nil {
		t.Fatalf("Walk failed: %v", err)
	}

	// 验证 walked 列表包含所有写入的文件
	m := make(map[string]struct{}, len(walked))
	for _, p := range walked {
		m[p] = struct{}{}
	}
	for _, expected := range paths {
		if _, found := m[expected]; !found {
			t.Errorf("Walk missing %q, walked: %v", expected, walked)
		}
	}
}
