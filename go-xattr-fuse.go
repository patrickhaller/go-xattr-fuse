package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/boltdb/bolt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/patrickhaller/slog"
)

type xattrFs struct {
	pathfs.FileSystem
}

var db *bolt.DB

func (x *xattrFs) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	slog.D("setxattr bucket `%s' name `%s'", name, attr)
	tx, err := db.Begin(true)
	if err != nil {
		slog.P("database cannot begin transaction: `%v'", err)
		return fuse.EBUSY
	}
	defer tx.Rollback()
	b, err := tx.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		slog.P("failed to create bucket `%s'", name)
		return fuse.EIO
	}
	b.Put([]byte(attr), data)
	if err := tx.Commit(); err != nil {
		slog.P("commit failed on `%s' attr `%s'", name, attr)
		return fuse.EIO
	}
	return fuse.OK
}

func boltBucket(name string) (*bolt.Tx, *bolt.Bucket, *bolt.Cursor, fuse.Status) {
	tx, err := db.Begin(true)
	if err != nil {
		slog.P("database cannot begin transaction: `%v'", err)
		return nil, nil, nil, fuse.EBUSY
	}
	b := tx.Bucket([]byte(name))
	if b == nil {
		return tx, nil, nil, fuse.ENOENT
	}
	return tx, b, b.Cursor(), fuse.OK
}

func (x *xattrFs) GetXAttr(name string, attr string, context *fuse.Context) ([]byte, fuse.Status) {
	slog.D("getxattr bucket `%s' name `%s'", name, attr)
	tx, _, c, err := boltBucket(name)
	defer tx.Rollback()
	if err != fuse.OK {
		return nil, err
	}
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if string(k) == attr {
			return v, fuse.OK
		}
	}
	return nil, fuse.OK
}

func (x *xattrFs) ListXAttr(name string, context *fuse.Context) ([]string, fuse.Status) {
	slog.D("listxattr bucket `%s'", name)
	tx, _, c, err := boltBucket(name)
	defer tx.Rollback()
	if err != fuse.OK {
		return nil, err
	}
	lis := make([]string, 1)
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		lis = append(lis, string(k))
	}
	slog.D("listxattr returns `%v'", lis)
	return lis[1:], fuse.OK
}

func (x *xattrFs) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
	slog.D("setxattr bucket `%s' name `%s'", name, attr)
	tx, b, _, err := boltBucket(name)
	defer tx.Rollback()
	if err != fuse.OK {
		return err
	}
	_ = b.Delete([]byte(attr))
	if err := tx.Commit(); err != nil {
		slog.P("commit failed on `%s' attr `%s'", name, attr)
		return fuse.EIO
	}
	return fuse.OK
}

// Begin overlay redirect functions
func (x *xattrFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	slog.D(name)
	return x.FileSystem.GetAttr(name, context)
}
func (x *xattrFs) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	slog.D(name)
	return x.FileSystem.Readlink(name, context)
}

func (x *xattrFs) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status {
	slog.D(name)
	return x.FileSystem.Mknod(name, mode, dev, context)
}

func (x *xattrFs) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	slog.D(name)
	return x.FileSystem.Mkdir(name, mode, context)
}

func (x *xattrFs) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	slog.D(name)
	return x.FileSystem.Unlink(name, context)
}

func (x *xattrFs) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
	slog.D(name)
	return x.FileSystem.Rmdir(name, context)
}

func (x *xattrFs) Symlink(value string, linkName string, context *fuse.Context) (code fuse.Status) {
	slog.D("%s -> %s", linkName, value)
	return x.FileSystem.Symlink(value, linkName, context)
}

func (x *xattrFs) Rename(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
	slog.D("%s -> %s", oldName, newName)
	return x.FileSystem.Rename(oldName, newName, context)
}

func (x *xattrFs) Link(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
	slog.D("%s -> %s", oldName, newName)
	return x.FileSystem.Link(oldName, newName, context)
}

func (x *xattrFs) Chmod(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	slog.D(name)
	return x.FileSystem.Chmod(name, mode, context)
}

func (x *xattrFs) Chown(name string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	slog.D(name)
	return x.FileSystem.Chown(name, uid, gid, context)
}

func (x *xattrFs) Truncate(name string, offset uint64, context *fuse.Context) (code fuse.Status) {
	slog.D(name)
	return x.FileSystem.Truncate(name, offset, context)
}

func (x *xattrFs) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	slog.D(name)
	return x.FileSystem.Open(name, flags, context)
}

func (x *xattrFs) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
	slog.D(name)
	return x.FileSystem.OpenDir(name, context)
}

func (x *xattrFs) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	slog.D(name)
	return x.FileSystem.Access(name, mode, context)
}

func (x *xattrFs) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	slog.D(name)
	return x.FileSystem.Create(name, flags, mode, context)
}

func (x *xattrFs) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) (code fuse.Status) {
	slog.D(name)
	return x.FileSystem.Utimens(name, Atime, Mtime, context)
}

func (x *xattrFs) StatFs(name string) *fuse.StatfsOut {
	slog.D(name)
	return nil
}

func main() {
	flag.Parse()
	if len(flag.Args()) < 1 {
		fmt.Printf("Usage:\n  %s DATABASE DIRECTORY MOUNTPOINT\n", os.Args[0])
		os.Exit(1)
	}
	dbFilename := flag.Arg(0)
	xattrlessDirectory := flag.Arg(1)
	mountpoint := flag.Arg(2)

	slog.Init(slog.Config{
		File:   "STDERR",
		Debug:  os.Getenv("DEBUG") != "",
		Prefix: "xAttrFS",
	})
	slog.D("using database `%s'", dbFilename)
	_db, err := bolt.Open(dbFilename, 0600, nil)
	db = _db
	if err != nil {
		slog.P("failed to open db: `%s'", err)
		os.Exit(1)
	}

	slog.D("using underlying directory `%s'", xattrlessDirectory)
	slog.D("mounting on `%s'", mountpoint)
	nfs := pathfs.NewPathNodeFs(&xattrFs{FileSystem: pathfs.NewLoopbackFileSystem(xattrlessDirectory)}, nil)
	conn := nodefs.NewFileSystemConnector(nfs.Root(), nil)
	server, err := fuse.NewServer(conn.RawFS(), mountpoint, &fuse.MountOptions{
		AllowOther: true,
	})
	if err != nil {
		slog.P("failed to mount `%s' on `%s': %v\n", xattrlessDirectory, mountpoint, err)
		os.Exit(1)
	}

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		server.Unmount()
	}()

	slog.D("now handling filesystem requests")
	server.Serve()
	slog.D("unmounting, and shutting down db")
	db.Close()
}
