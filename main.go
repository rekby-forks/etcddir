package main

import (
	"bytes"
	"fmt"
	"github.com/coreos/etcd/client"
	"github.com/rjeczalik/notify"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
	"golang.org/x/net/context"
	"flag"
)

const MARK_FILE_NAME = ".ETCDIR_MARK_FILE_HUGSDBDND" // Name of lock-file for prevent bad things
const DEFAULT_DIRMODE = 0777
const DEFAULT_FILEMODE = 0777
const EVENT_CHANNEL_LEN = 1000
const LOCK_INTERVAL = time.Second // Wait since previous touch to can get lock directory once more.

var (
	serverAddr = flag.String("server", "http://localhost:2379", "Client url for one of cluster servers")
	serverRootDir = flag.String("root", "/", "server root dir for map")
)

type fileChangeEvent struct {
	Path      string
	IsDir     bool
	IsRemoved bool
	Content   []byte
}

/*
Monitoring changes in etcd server.
It designed for run in separate goroutine.
 */
func etcdMon(etcdRootPath string, config client.Config, bus chan fileChangeEvent, startIndex uint64) {
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
Monitoring changes in file system.
It designed for run in separate goroutine.
 */
func fileMon(path string, bus chan fileChangeEvent) {
	// Make the channel buffered to ensure no event is dropped. Notify will drop
	// an event if the receiver is not able to keep up the sending pace.
	c := make(chan notify.EventInfo, 1)

	// Set up a watchpoint listening on events within current working directory.
	// Dispatch each create and remove events separately to c.
	if err := notify.Watch(path+"/...", c, notify.All); err != nil {
		log.Fatal(err)
	}
	defer notify.Stop(c)

	// Block until an event is received.
	for {
		event := <-c
		fstat, err := os.Lstat(event.Path())
		if os.IsNotExist(err) {
			bus <- fileChangeEvent{Path: event.Path(), IsRemoved: true}
			continue
		}

		if err != nil {
			log.Println(err)
			continue
		}

		if fstat.IsDir() {
			bus <- fileChangeEvent{Path: event.Path(), IsDir: true}
			continue
		}

		content, err := ioutil.ReadFile(event.Path())
		if err != nil {
			log.Println(err)
		}
		bus <- fileChangeEvent{Path: event.Path(), Content: content}
	}
}


/*
Clear dir and dump content of etcd to the dir.
ATTENTION: the function REMOVE ALL CONTENT of the dir.
 */
func firstSyncEtcDir(etcdRootPath string, etcdConfig client.Config, dir string) (etcdIndex uint64) {
	dirFile, err := os.Open(dir)
	if err != nil {
		panic(err)
	}
	dirNames, err := dirFile.Readdirnames(-1)
	if err != nil {
		panic(err)
	}
	for _, item := range dirNames {
		err = os.RemoveAll(filepath.Join(dir, item))
		if err != nil {
			log.Println("I can't remove: ", filepath.Join(dir, item))
			panic(err)
		}
	}
	err = ioutil.WriteFile(filepath.Join(dir, MARK_FILE_NAME), []byte{}, DEFAULT_FILEMODE)
	if err != nil {
		log.Println("I can't create touchfile: ", filepath.Join(dir, MARK_FILE_NAME))
		return
	}

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
Check if dir can be locked and lock it.
Return true if lock succesfully.
 */
func lock(dir string)bool{
	lockFile := filepath.Join(dir, MARK_FILE_NAME)
	stat, err := os.Stat(lockFile)
	if err != nil {
		log.Println("Can't stat lock file: ", lockFile, err)
		return false
	}
	if time.Now().Sub(stat.ModTime()) <= LOCK_INTERVAL {
		return false
	}

	pid := os.Getpid()
	go func(){
		for {
			mess := fmt.Sprint("PID: ", pid, "\nLAST TIME: ", time.Now().String())
			ioutil.WriteFile(lockFile, []byte(mess), DEFAULT_FILEMODE)
			time.Sleep(LOCK_INTERVAL / 3)
		}
	}()
	return true
}

func main() {
	flag.Usage = printUsage
	flag.Parse()
	if flag.NArg() != 1 {
		printUsage()
		return
	}

	dir, err := filepath.Abs(flag.Arg(0))
	if err != nil {
		panic(err)
	}
	dirStat, err := os.Stat(dir)
	if err != nil {
		panic(err)
	}
	if !dirStat.IsDir() {
		fmt.Printf("'%v' is not dir.\n", dir)
		return
	}

	_, err = os.Stat(filepath.Join(dir, MARK_FILE_NAME))
	if os.IsNotExist(err) {
		fmt.Printf(`You have to create file '%[1]v' before usage dir as syncdir. You can do it by command:
echo > %[1]v
`, filepath.Join(dir, MARK_FILE_NAME))
		return
	}

	if !lock(dir){
		log.Println("Can't get lock. May be another instance work with the dir")
		return
	}

	fmt.Println(os.Args)
	etcdConfig := client.Config{Endpoints: []string{*serverAddr}}
	fmt.Println(etcdConfig)
	etcdStartFrom := firstSyncEtcDir(*serverRootDir, etcdConfig, dir)

	etcdChan := make(chan fileChangeEvent, EVENT_CHANNEL_LEN)
	fsChan := make(chan fileChangeEvent, EVENT_CHANNEL_LEN)

	go fileMon(dir, fsChan)
	go etcdMon(*serverRootDir, etcdConfig, etcdChan, etcdStartFrom)

	syncProcess(dir, *serverRootDir, etcdConfig, etcdChan, fsChan)
}

func printUsage() {
	fmt.Printf(`%v [options] <syncdir>
syncdir - directory for show etcd content. ALL CURRENT CONTENT WILL BE LOST.
you have to create file '%v' in syncdir before can use it.
`, os.Args[0], MARK_FILE_NAME)
	flag.PrintDefaults()
}

/*
function for replicate changes between etcd and file system.
It is never returned function.
It can be run in separate goroutine or call it as last function in main()
 */
func syncProcess(dir, etcdRootDir string, etcdConfig client.Config, etcdChan, fsChan <-chan fileChangeEvent) {
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
