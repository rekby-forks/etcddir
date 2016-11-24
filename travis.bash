#!/usr/bin/env bash

# Prepare
wget https://github.com/coreos/etcd/releases/download/v3.0.13/etcd-v3.0.13-linux-amd64.tar.gz
tar zxf etcd-v3.0.13-linux-amd64.tar.gz
mv etcd-v3.0.13-linux-amd64/etcd ./
mv etcd-v3.0.13-linux-amd64/etcdctl ./

go build -v
export ETCDCTL_API=3

function restart(){
    PREFIX="$1"
    killall -9 etcd
    killall -9 etcddir
    rm -rf default.etcd
    rm -rf sync

    mkdir sync
    touch sync/.ETCDIR_MARK_FILE_HUGSDBDND
    ./etcd &
    sleep 1
    ./etcddir "--prefix=$PREFIX" sync &
    sleep 1
}

function report(){
    echo "REPORT:" "$1"
    exit 1
}

restart

echo TEST-3-Simple
./etcdctl put aaa bbb
sleep 1
ps aux | grep etcd
ls -la sync
[ "bbb" == `cat sync/aaa`"" ] || report aaa

restart
echo TEST-3-Pull-3
mkdir test
echo test1 > test/a
echo test2 > test/b
echo test3 > test/c

cp -R test sync/test
sleep 1

echo "ls"
ls sync/test

echo "get all"
./etcdctl get "a" "z"

TEST1=`./etcdctl get test/a | tail -n +2`
echo "TEST1: ${TEST1}"
[ "test1" == "${TEST1}" ] || report test1

TEST2=`./etcdctl get test/b | tail -n +2`
echo "TEST2: ${TEST2}"
[ "test2" == "${TEST2}" ] || report test2

TEST3=`./etcdctl get test/c | tail -n +2`
echo "TEST3: ${TEST3}"
[ "test3" == "${TEST3}" ] || report test3
