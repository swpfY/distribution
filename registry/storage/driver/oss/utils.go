package oss

import (
	"strconv"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
)

func (d *driver) ossBucket() *string {
	return oss.Ptr(d.bucket)
}

func (d *driver) ossKey(path string) *string {
	key := d.pathToKey(path)
	return oss.Ptr(key)
}

func (d *driver) pathToKey(path string) string {
	clean := strings.Trim(path, "/")
	if d.rootDirectory == "" {
		return clean
	}
	return strings.TrimRight(d.rootDirectory, "/") + "/" + clean
}

func (d *driver) keyToPath(key string) string {
	clean := strings.Trim(key, "/")
	prefix := strings.Trim(d.rootDirectory, "/")
	if prefix == "" {
		return "/" + clean
	}
	// 根目录
	if clean == prefix {
		return "/"
	}
	return "/" + strings.TrimPrefix(clean, prefix+"/")
}

func (d *driver) ossRange(offset int64) *string {
	ofs := strconv.FormatInt(offset, 10)
	return oss.Ptr("bytes=" + ofs + "-")
}
