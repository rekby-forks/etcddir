package main

import (
	"time"
	"github.com/coreos/etcd/client"
	"log"
	"golang.org/x/net/context"
	"fmt"
	"path/filepath"
	"bytes"
	"strings"
	"os"
	"io/ioutil"
)

/*
Monitoring changes in etcd server.
It designed for run in separate goroutine.
 */
func etcdMon_v2(etcdRootPath string, config client.Config, bus chan fileChangeEvent, startIndex uint64) {
	c, err := client.New(config)
	if err != nil {
		panic(err)
	}
	kapi := client.NewKeysAPI(c)
	var nextEvent uint64 = startIndex
	for {
		response, err := kapi.Watcher(etcdRootPath, &client.WatcherOptions{AfterIndex: nextEvent, Recursive: true}).Next(context.Background())
		if err != nil {
			log.Println(err)
			time.Sleep(time.Second)
			continue
		}
		nextEvent = response.Index
		if response.Action == "delete" {
			bus <- fileChangeEvent{Path: response.Node.Key, IsRemoved: true, IsDir: response.Node.Dir}
			continue
		}
		if response.Node.Dir {
			bus <- fileChangeEvent{Path: response.Node.Key, IsDir: response.Node.Dir}
			continue
		}
		bus <- fileChangeEvent{Path: response.Node.Key, Content: []byte(response.Node.Value)}
	}
}

/*
Clear dir and dump content of etcd to the dir.
ATTENTION: the function REMOVE ALL CONTENT of the dir.
 */
func firstSyncEtcDir_v2(etcdRootPath string, etcdConfig client.Config, dir string) (etcdIndex uint64) {
	cleanDir(dir)

	etcdClient, err := client.New(etcdConfig)
	if err != nil {
		log.Println("Can't create etcdClient: ", err)
		panic(err)
	}

	kapi := client.NewKeysAPI(etcdClient)
	response, err := kapi.Get(context.Background(), etcdRootPath, &client.GetOptions{Recursive: true, Quorum: true})
	if err != nil {
		fmt.Println("I can't get initial etcd state: ", err)
		panic(err)
	}
	writeNodeToDir(dir, etcdRootPath, response.Node)
	etcdIndex = response.Index
	return
}

/*
function for replicate changes between etcd and file system.
It is never returned function.
It can be run in separate goroutine or call it as last function in main()
 */
func syncProcess_v2(dir, etcdRootDir string, etcdConfig client.Config, etcdChan, fsChan <-chan fileChangeEvent) {
	etcdClient, err := client.New(etcdConfig)
	if err != nil {
		panic(err)
	}
	kapi := client.NewKeysAPI(etcdClient)
	ctx := context.Background()
	fsMarkFile := filepath.Join(dir, MARK_FILE_NAME)
	for {
		var event fileChangeEvent
		select {
		case event = <-etcdChan:
			fsPath := filepath.Join(dir, event.Path)
			if fsPath == fsMarkFile {
				continue
			}
			switch {
			case event.IsRemoved:
				err := os.RemoveAll(fsPath)
				if err != nil {
					log.Println("Can't remove: ", fsPath, err)
				}
			case event.IsDir:
				err := os.Mkdir(fsPath, DEFAULT_DIRMODE)
				if err != nil && !os.IsExist(err){
					log.Println("Can't make dir: ", fsPath, err)
				}
			default:
				if content, err := ioutil.ReadFile(fsPath); err == nil {
					if bytes.Equal(content, event.Content) {
						// Skip if contents are equals
						continue
					}
				}
				err := ioutil.WriteFile(fsPath, event.Content, DEFAULT_FILEMODE)
				if err != nil {
					log.Println("Can't write file: ", fsPath, err)
				}
			}
		case event = <-fsChan:
			if event.Path == fsMarkFile {
				continue
			}
			etcdPath := etcdRootDir + event.Path[len(dir):]
			etcdPath = strings.Replace(etcdPath, "\\", "/", -1)
			switch {
			case event.IsRemoved:
				_, err := kapi.Delete(ctx, etcdPath, &client.DeleteOptions{Recursive: true})
				if err != nil {
					log.Println("Can't remove etcd: "+etcdPath, err)
				}
			case event.IsDir:
				_, err := kapi.Set(ctx, etcdPath, "", &client.SetOptions{Dir: true})
				if err != nil {
					log.Println("Can't create etcd dir: ", etcdPath, err)
				}
			default:
				if resp, err := kapi.Get(ctx, etcdPath, &client.GetOptions{Quorum: true}); err == nil {
					if bytes.Equal([]byte(resp.Node.Value), event.Content) {
						// Skip equal contents
						continue
					}
				}
				_, err := kapi.Set(ctx, etcdPath, string(event.Content), nil)
				if err != nil {
					log.Println("Can't set etcd value: ", etcdPath, err)
				}
			}
		}
	}
}


func writeNodeToDir(dir, root string, node *client.Node) {
	//log.Println("I can't create dir:  ", node.Key," ", dir, "root:", root)
	nodePath := filepath.Join(dir, node.Key[len(root)-1:])
	//log.Println("I can't create dir: ", nodePath, " ", node.Key," ", dir, " ", root)
	if node.Dir {
		err := os.Mkdir(nodePath, DEFAULT_DIRMODE)
		if err != nil && !os.IsExist(err) {
			log.Println("I can't create dir: ", nodePath, " ", node.Key," ", dir, " ", root)
			panic(err)
		}
		for _, item := range node.Nodes {
			writeNodeToDir(dir, root, item)
		}
	} else {
		err := ioutil.WriteFile(nodePath, []byte(node.Value), DEFAULT_FILEMODE)
		if err != nil {
			log.Println("I can't create file: ", nodePath)
			panic(err)
		}
	}
}
