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

Default it is connect to localhost, port 2379 with api version 3.
You can change the servres address and api version.
It doesn't support any authentication now. If you need - welcome issue or pull-request.
If you need normal authenticate mode - write me or create pull request.


Now it is compete ignore dirs while sync for api v3 (sync only files content).
Now it isn't support for have key, which is directory in map semantic.

For example if etcd have keys:
    
    asd
    asd/123.txt
    qqq/222.txt
    fff.txt
    
then it will create files qqq/222.txt and fff.txt.
For asd/123.txt is undefined behaviour: it may create\update the file, but may not.
