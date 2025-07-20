package oss

import (
	"strconv"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
)

func (d *driver) bucketPtr() *string {
	return oss.Ptr(d.bucket)
}

func (d *driver) pathToKeyPtr(path string) *string {
	key := d.pathToKey(path)
	return oss.Ptr(key)
}

func (d *driver) pathToKey(path string) string {
	clean := strings.Trim(path, "/")

	if d.rootDirectory == "" {
		return clean
	}
	if clean == "" {
		return d.rootDirectory
	}
	return d.rootDirectory + "/" + clean
}

func (d *driver) keyToPath(key string) string {
	clean := strings.Trim(key, "/")
	if d.rootDirectory == "" {
		return "/" + clean
	}
	if clean == d.rootDirectory {
		return "/"
	}
	return "/" + strings.TrimPrefix(clean, d.rootDirectory+"/")
}

func (d *driver) folderPathToKey(path string) string {
	clean := strings.Trim(path, "/")

	if d.rootDirectory == "" && clean == "" {
		return "/"
	}
	if d.rootDirectory == "" {
		return clean + "/"
	}
	if clean == "" {
		return d.rootDirectory + "/"
	}
	return d.rootDirectory + "/" + clean + "/"
}

func (d *driver) folderKeyToPath(key string) string {
	clean := strings.Trim(key, "/")

	if d.rootDirectory == "" && clean == "" {
		return "/"
	}
	if d.rootDirectory == "" {
		return "/" + clean
	}
	if clean == d.rootDirectory {
		return "/"
	}
	return "/" + strings.TrimPrefix(clean, d.rootDirectory+"/") + "/"
}

func (d *driver) ossRangePtr(offset int64) *string {
	ofs := strconv.FormatInt(offset, 10)
	return oss.Ptr("bytes=" + ofs + "-")
}
