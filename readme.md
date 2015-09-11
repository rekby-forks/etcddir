File-system cross-platform interface for etcd.

It isn't production ready and use myself for simple interface for etcd under windows, where I can't use etcd-fuse
https://github.com/xetorthio/etcd-fs

For usage:

1. download binary or compile sources

2. create directory for sync with etcd

3. create MARK-file .ETCDIR_MARK_FILE_HUGSDBDND in the directory

4. run etcdir <PATH_TO_DIRECTORY_WITH_MARK_FILE>

example:
```
mkdir c:\\tmp\\etcdir
echo > c:\\tmp\\etcdir\\.ETCDIR_MARK_FILE_HUGSDBDND
etcdir c:\\tmp\\etcdir
```

The mark-file need for prevent fatal-error: etcdir REMOVE ALL CONTENTS from synced dir when start and replace it from etcd.


Now it work only with default etcd: http://127.0.0.1:4001 without authentication - for work with local server.
If you need normal authenticate mode - write me or create pull request.
