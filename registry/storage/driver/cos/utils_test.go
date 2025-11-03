package cos

import "testing"

func TestPathToKey(t *testing.T) {
	cases := []struct {
		root string
		path string
		key  string
	}{
		{root: "", path: "/a/b/c.txt", key: "a/b/c.txt"},
		{root: "", path: "a/b/c.txt", key: "a/b/c.txt"},
		{root: "", path: "/", key: ""},
		{root: "rootdir", path: "/a/b/c.txt", key: "rootdir/a/b/c.txt"},
		{root: "rootdir", path: "a/b/c.txt", key: "rootdir/a/b/c.txt"},
		{root: "rootdir", path: "/", key: "rootdir"},
		{root: "dup-rootdir", path: "/dup-rootdir/a/b/c.txt", key: "dup-rootdir/a/b/c.txt"},
		{root: "dup-rootdir", path: "dup-rootdir/a/b/c.txt", key: "dup-rootdir/a/b/c.txt"},
	}

	for _, c := range cases {
		key := PathToKey(c.root, c.path)
		if key != c.key {
			t.Errorf("PathToKey(%q, %q) = %q; want %q", c.root, c.path, key, c.key)
		}
	}
}

func TestKeyToPath(t *testing.T) {
	cases := []struct {
		root string
		key  string
		path string
	}{
		{root: "", key: "a/b/c.txt", path: "/a/b/c.txt"},
		{root: "", key: "a/b/c.txt", path: "/a/b/c.txt"},
		{root: "", key: "/", path: "/"},
		{root: "rootdir", key: "rootdir/a/b/c.txt", path: "/a/b/c.txt"},
		{root: "rootdir", key: "rootdir/a/b/c.txt", path: "/a/b/c.txt"},
		{root: "rootdir", key: "rootdir", path: "/"},
		{root: "dup-rootdir", key: "dup-rootdir/a/b/c.txt", path: "/a/b/c.txt"},
		{root: "dup-rootdir", key: "dup-rootdir/a/b/c.txt", path: "/a/b/c.txt"},
	}

	for _, c := range cases {
		path := KeyToPath(c.root, c.key)
		if path != c.path {
			t.Errorf("KeyToPath(%q, %q) = %q; want %q", c.root, c.key, path, c.path)
		}
	}
}
