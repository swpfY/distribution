package cos

import (
	"path/filepath"
	"slices"
	"strings"
	"time"
)

func ParseTime(timeStr string) (time.Time, error) {
	var timeObj time.Time
	timeObj, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		timeObj, err = time.Parse(time.RFC1123, timeStr)
		if err != nil {
			return time.Time{}, err
		}
	}
	return timeObj, nil
}

// DirectoryDiff finds all directories that are not in common between
// the previous and current paths in sorted order.
//
// # Examples
//
//	DirectoryDiff("/path/to/folder", "/path/to/folder/folder/file")
//	// => [ "/path/to/folder/folder" ]
//
//	DirectoryDiff("/path/to/folder/folder1", "/path/to/folder/folder2/file")
//	// => [ "/path/to/folder/folder2" ]
//
//	DirectoryDiff("/path/to/folder/folder1/file", "/path/to/folder/folder2/file")
//	// => [ "/path/to/folder/folder2" ]
//
//	DirectoryDiff("/path/to/folder/folder1/file", "/path/to/folder/folder2/folder1/file")
//	// => [ "/path/to/folder/folder2", "/path/to/folder/folder2/folder1" ]
//
//	DirectoryDiff("/", "/path/to/folder/folder/file")
//	// => [ "/path", "/path/to", "/path/to/folder", "/path/to/folder/folder" ]
func DirectoryDiff(prev, current string) []string {
	var paths []string

	if prev == "" || current == "" {
		return paths
	}

	parent := current
	for {
		parent = filepath.Dir(parent)
		if parent == "/" || parent == prev || strings.HasPrefix(prev+"/", parent+"/") {
			break
		}
		paths = append(paths, parent)
	}
	slices.Reverse(paths)
	return paths
}

// PathToKey 将路径转换为存储键
func PathToKey(rootDir, path string) string {
	rootDir = strings.Trim(rootDir, "/")
	path = strings.Trim(path, "/")
	// Important! delete the root prefix if existed
	path = strings.TrimLeft(strings.TrimPrefix(path, rootDir), "/")
	if rootDir == "" {
		return path
	}
	if path == "" {
		return rootDir
	}
	return rootDir + "/" + path
}

// KeyToPath 将存储键转换为路径
func KeyToPath(rootDir, key string) string {
	rootDir = strings.Trim(rootDir, "/")
	key = strings.Trim(key, "/")
	if rootDir == "" {
		return "/" + key
	}
	if key == "" {
		return ""
	}
	return "/" + strings.TrimLeft(strings.TrimPrefix(key, rootDir), "/")
}
