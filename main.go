package main

import (
	"flag"
	"fmt"
	"github.com/coreos/etcd/client"
	"github.com/rjeczalik/notify"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"
	"github.com/coreos/etcd/clientv3"
)

const MARK_FILE_NAME = ".ETCDIR_MARK_FILE_HUGSDBDND" // Name of lock-file for prevent bad things
const DEFAULT_DIRMODE = 0777
const DEFAULT_FILEMODE = 0777
const EVENT_CHANNEL_LEN = 1000
const LOCK_INTERVAL = time.Second // Wait since previous touch to can get lock directory once more.

var (
	serverAddr    = flag.String("server", "http://localhost:2379", "Client url for one of cluster servers")
	serverRootDir = flag.String("root", "/", "server root dir for map")
	apiVersion    = flag.Int("api", 3, "Api version 2 or 3")
)

type fileChangeEvent struct {
	Path      string
	IsDir     bool
	IsRemoved bool
	Content   []byte
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

func cleanDir(dir string) {
	dirFile, err := os.Open(dir)
	if err != nil {
		panic(err)
	}
	dirNames, err := dirFile.Readdirnames(-1)
	if err != nil {
		panic(err)
	}
	for _, item := range dirNames {
		if item == MARK_FILE_NAME {
			continue
		}
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
}

/*
Check if dir can be locked and lock it.
Return true if lock succesfully.
*/
func lock(dir string) bool {
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
	go func() {
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

	if !lock(dir) {
		log.Println("Can't get lock. May be another instance work with the dir")
		return
	}

	fmt.Println(os.Args)
	fsChan := make(chan fileChangeEvent, EVENT_CHANNEL_LEN)
	go fileMon(dir, fsChan)

	switch *apiVersion {
	case 2:
		etcdConfig := client.Config{Endpoints: []string{*serverAddr}}
		fmt.Printf("%#v\n", etcdConfig)
		etcdStartFrom := firstSyncEtcDir_v2(*serverRootDir, etcdConfig, dir)
		etcdChan := make(chan fileChangeEvent, EVENT_CHANNEL_LEN)
		go etcdMon_v2(*serverRootDir, etcdConfig, etcdChan, etcdStartFrom)

		syncProcess_v2(dir, *serverRootDir, etcdConfig, etcdChan, fsChan)
	case 3:
		etcdConfig := clientv3.Config{
			Endpoints:[]string{*serverAddr},
		}
		fmt.Printf("%#v\n", etcdConfig)
		c3, err := clientv3.New(etcdConfig)
		if err != nil {
			panic(err)
		}
		firstSyncEtcDir_v3(*serverRootDir, c3, dir)
	default:
		panic("Unsupported API version")
	}
}

func printUsage() {
	fmt.Printf(`%v [options] <syncdir>
syncdir - directory for show etcd content. ALL CURRENT CONTENT WILL BE LOST.
you have to create file '%v' in syncdir before can use it.
`, os.Args[0], MARK_FILE_NAME)
	flag.PrintDefaults()
}
