package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
)

const (
	// constant vendor
	// copy from https://github.com/coreos/etcd/blob/80d15948bcfc93aabd2c5245d7993c8a9e76bf8f/internal/mvcc/mvccpb/kv.pb.go
	//PUT    Event_EventType = 0
	//DELETE Event_EventType = 1
	ETCD_EVENT_PUT    = 0
	ETCD_EVENT_DELETE = 1
)

func etcdMon_v3(prefix string, c3 *clientv3.Client, bus chan fileChangeEvent, startRevision int64) {
	key, option := prefixToKeyOption(prefix)
	ch := c3.Watch(context.Background(), key, option, clientv3.WithRev(startRevision))
	for chEvent := range ch {
		for _, event := range chEvent.Events {
			fileEvent := fileChangeEvent{
				Path:    string(event.Kv.Key),
				Content: event.Kv.Value,
			}
			event.IsCreate()
			switch int(event.Type) {
			case ETCD_EVENT_PUT:
				bus <- fileEvent
			case ETCD_EVENT_DELETE:
				fileEvent.IsRemoved = true
				bus <- fileEvent
			default:
				log.Println("etcdMon_v3 undefined event type: ", event.Type)
			}
		}
	}
	close(bus)
}

/*
Sync localdir to etcd server state.
WARNING: ALL CONTENT OF localdir WILL BE LOST

Return revision of synced state
*/
func firstSyncEtcDir_v3(prefix string, c *clientv3.Client, localdir string) int64 {
	cleanDir(localdir)

	key, option := prefixToKeyOption(prefix)

	// Get all values
	resp, err := c.Get(context.Background(), key, option, clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		panic(err)
	}

	for _, kv := range resp.Kvs {
		targetPath := keyToLocalPath(strings.TrimPrefix(string(kv.Key), prefix), localdir)
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
func keyToLocalPath(key string, localdir string) string {
	if !utf8.ValidString(key) {
		log.Printf("Key skip, becouse it isn't valid utf8: %x\n", key)
		return ""
	}
	targetPath := filepath.Clean(filepath.Join(localdir, key))
	if !strings.HasPrefix(targetPath, localdir+string(filepath.Separator)) {
		log.Printf("Key skip, becouse it out of base directory. Key: '%s', TargetPath: '%s'\n, base: '%s'\n", key, targetPath, localdir)
		return ""
	}
	return targetPath
}

func prefixToKeyOption(prefix string) (key string, option clientv3.OpOption) {
	if prefix == "" {
		key = "\x00"
		option = clientv3.WithFromKey()
	} else {
		key = prefix
		option = clientv3.WithPrefix()
	}

	return key, option
}

func syncProcess_v3(localDir string, serverPrefix string, c3 *clientv3.Client, etcdChan, fsChan <-chan fileChangeEvent) {
	fsMarkFile := filepath.Join(localDir, MARK_FILE_NAME)
	for {
		select {
		case event := <-etcdChan:
			filePath := keyToLocalPath(strings.TrimPrefix(event.Path, serverPrefix), localDir)
			if filePath == "" || filePath == fsMarkFile {
				continue
			}
			if event.IsRemoved {
				os.RemoveAll(filePath)
				//fmt.Println("Remove: " + filePath)
			} else {
				fileContent, err := ioutil.ReadFile(event.Path)
				if err == nil && bytes.Equal(fileContent, event.Content) {
					continue
				}

				dirName := filepath.Dir(event.Path)
				os.MkdirAll(dirName, DEFAULT_DIRMODE)
				err = ioutil.WriteFile(filePath, event.Content, DEFAULT_FILEMODE)
				if err != nil {
					log.Printf("syncProcess_v3 error while put file '%v': %v\n", event.Path, err)
				}
			}
		case event := <-fsChan:
			if event.Path == fsMarkFile {
				continue
			}
			syncProcess_v3FSEvent(localDir, serverPrefix, c3, event)

		}
	}
}

func syncProcess_v3FSEvent(localDir string, serverPrefix string, c3 *clientv3.Client, event fileChangeEvent) {
	etcdPath, err := filepath.Rel(localDir, event.Path)
	if err != nil {
		log.Printf("syncProcess_v3 error get relpath '%v': %v\n", event.Path, err)
		return
	}
	etcdPath = serverPrefix + etcdPath
	etcdPath = strings.Replace(etcdPath, string(os.PathSeparator), "/", -1)

	switch {
	case event.IsRemoved:
		_, err := c3.Delete(context.Background(), etcdPath)
		if err != nil {
			log.Printf("syncProcess_v3 error while delete etcdkey '%v': %v\n", etcdPath, err)
		}
	case event.IsDir:
		files, _ := ioutil.ReadDir(event.Path)
		for _, file := range files {
			path := filepath.Join(event.Path, file.Name())
			content := []byte(nil)
			if !file.IsDir() {
				content, err = ioutil.ReadFile(path)
				if err != nil {
					log.Println(err)
				}
			}
			syncProcess_v3FSEvent(localDir, serverPrefix, c3, fileChangeEvent{
				Path:      path,
				IsDir:     file.IsDir(),
				IsRemoved: false,
				Content:   content,
			})
		}
	case !event.IsDir:
		resp, err := c3.Get(context.Background(), etcdPath)
		if err != nil {
			log.Printf("syncProcess_v3 Can't read key '%v': %v\n", etcdPath, err)
		}
		if len(resp.Kvs) > 0 {
			if bytes.Equal(resp.Kvs[0].Value, event.Content) {
				return
			}
		}
		_, err = c3.Put(context.Background(), etcdPath, string(event.Content))
		if err != nil {
			log.Printf("syncProcess_v3 error while put etcdkey '%v': %v\n", etcdPath, err)
		}
	}
}
