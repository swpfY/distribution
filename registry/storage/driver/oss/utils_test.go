package oss

import (
	"strconv"
	"testing"
)

func TestPathToKey(t *testing.T) {
	d := &driver{rootDirectory: "registry"}

	tests := []struct {
		path     string
		expected string
	}{
		{"/abc/def", "registry/abc/def"},
		{"/abc", "registry/abc"},
		{"abc/def", "registry/abc/def"},
		{"/abc/def/", "registry/abc/def"},
	}

	for _, tt := range tests {
		if key := d.pathToKey(tt.path); key != tt.expected {
			t.Errorf("pathToKey(%q) = %q; want %q", tt.path, key, tt.expected)
		}
	}
}

func TestKeyToPath(t *testing.T) {
	d := &driver{rootDirectory: "registry"}

	tests := []struct {
		key      string
		expected string
	}{
		{"registry/abc/def", "/abc/def"},
		{"registry/abc", "/abc"},
		{"registry/", "/"},
	}

	for _, tt := range tests {
		if path := d.keyToPath(tt.key); path != tt.expected {
			t.Errorf("keyToPath(%q) = %q; want %q", tt.key, path, tt.expected)
		}
	}
}

func TestFolderPathToKey(t *testing.T) {
	d := &driver{rootDirectory: "registry"}

	tests := []struct {
		path     string
		expected string
	}{
		{"/abc/def", "registry/abc/def/"},
		{"abc", "registry/abc/"},
		{"/", "registry/"},
	}

	for _, tt := range tests {
		if key := d.folderPathToKey(tt.path); key != tt.expected {
			t.Errorf("folderPathToKey(%q) = %q; want %q", tt.path, key, tt.expected)
		}
	}
}

func TestFolderKeyToPath(t *testing.T) {
	d := &driver{rootDirectory: "registry"}

	tests := []struct {
		key      string
		expected string
	}{
		{"registry/abc/def", "/abc/def/"},
		{"registry/abc/", "/abc/"},
		{"registry/", "/"},
	}

	for _, tt := range tests {
		if path := d.folderKeyToPath(tt.key); path != tt.expected {
			t.Errorf("folderKeyToPath(%q) = %q; want %q", tt.key, path, tt.expected)
		}
	}
}

func TestOssBucketPtr(t *testing.T) {
	d := &driver{bucket: "my-bucket"}
	ptr := d.bucketPtr()

	if ptr == nil || *ptr != "my-bucket" {
		t.Errorf("bucketPtr() = %v; want 'my-bucket'", ptr)
	}
}

func TestOssKeyPtr(t *testing.T) {
	d := &driver{rootDirectory: "registry"}
	ptr := d.pathToKeyPtr("/abc/def")

	if ptr == nil || *ptr != "registry/abc/def" {
		t.Errorf("ossKeyPtr() = %v; want 'registry/abc/def'", ptr)
	}
}

func TestOssRange(t *testing.T) {
	d := &driver{}
	offset := int64(1024)
	ptr := d.ossRangePtr(offset)

	expected := "bytes=" + strconv.FormatInt(offset, 10) + "-"
	if ptr == nil || *ptr != expected {
		t.Errorf("ossRange(%d) = %v; want %q", offset, ptr, expected)
	}
}
