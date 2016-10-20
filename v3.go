package main

import (
	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

/*
Sync localdir to etcd server state.
WARNING: ALL CONTENT OF localdir WILL BE LOST

Return revision of synced state
*/
func firstSyncEtcDir_v3(prefix string, c *clientv3.Client, localdir string) int64 {
	cleanDir(localdir)

	var key string
	var option clientv3.OpOption
	if prefix == "" {
		key = "\x00"
		option = clientv3.WithFromKey()
	} else {
		key = prefix
		option = clientv3.WithPrefix()
	}

	// Get all values
	resp, err := c.Get(context.Background(), key, option, clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		panic(err)
	}

	for _, kv := range resp.Kvs {
		targetPath := keyToLocalPath(kv.Key, localdir)
		if targetPath == "" {
			continue
		}

		targetDir := filepath.Dir(targetPath)
		os.MkdirAll(targetDir, DEFAULT_DIRMODE)
		err = ioutil.WriteFile(targetPath, kv.Value, DEFAULT_FILEMODE)
		if err != nil {
			log.Printf("firstSyncEtcDir_v3 error write file '%v': %v\n", targetPath, err)
		}
	}
	return resp.Header.Revision
}

/*
return path to key in localdir.
Return "" if error.
*/
func keyToLocalPath(key []byte, localdir string) string {
	if !utf8.Valid(key) {
		log.Printf("Key skip, becouse it isn't valid utf8: %x\n", key)
		return ""
	}
	targetPath := filepath.Clean(filepath.Join(localdir, string(key)))
	if !strings.HasPrefix(targetPath, localdir+string(filepath.Separator)) {
		log.Printf("Key skip, becouse it out of base directory. Key: '%s', TargetPath: '%s'\n", key, targetPath)
		return ""
	}
	return targetPath
}
