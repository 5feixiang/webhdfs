package main

import (
	"bytes"
	"io/ioutil"
	"net"
	"net/http"
	"path"
	"reflect"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/xiekeyang/webhdfs"
)

const (
	nameNodeHost = "localhost"
	nameNodePort = "50070"
	hdfsUser     = "kxie"
	blockSize    = int64(64 << 20)
	bufferSize   = int32(4096)
	replication  = int16(1)
	fpath        = "/mytest/foo"
	f2path       = "/retest/bar"
	dirpath      = "/mytest"
)

var (
	BYTE_SRC    = []byte{1, 2, 3, 4, 5}
	READER_SRC  = bytes.NewReader(BYTE_SRC)
	BYTE2_SRC   = []byte{6, 7, 8, 9}
	READER2_SRC = bytes.NewReader(BYTE2_SRC)
)

func main() {
	fs := &webhdfs.FileSystem{
		NameNodeHost: nameNodeHost,
		NameNodePort: nameNodePort,
		UserName:     hdfsUser,
		BufferSize:   bufferSize,
		BlockSize:    blockSize,
		Replication:  replication,
		Client:       &http.Client{Transport: &http.Transport{Dial: dialTimeout}},
	}
	CreateAndRead(fs)
}

func CreateAndRead(fs *webhdfs.FileSystem) {
	if err := fs.Create(READER_SRC, fpath); err != nil {
		log.Errorf("%s", err)
		return
	} else {
		log.Infof("Created!")
	}
	if rc, err := fs.Open(fpath, 0, int64(len(BYTE_SRC))); err != nil {
		log.Errorf("%s", err)
		return
	} else {
		b, err := ioutil.ReadAll(rc)
		if err != nil {
			log.Errorf("%s", err)
			return
		} else {
			log.Infof("[CreateAndRead] %v", b)
		}
	}
}

func AppendAndRead(fs *webhdfs.FileSystem) {
	if err := fs.Append(READER2_SRC, fpath); err != nil {
		log.Errorf("%s", err)
		return
	} else {
		log.Infof("Append!")
	}
	if rc, err := fs.Open(fpath, 0, int64(len(BYTE_SRC)+len(BYTE2_SRC))); err != nil {
		log.Errorf("%s", err)
		return
	} else {
		b, err := ioutil.ReadAll(rc)
		if err != nil {
			log.Errorf("%s", err)
			return
		} else {
			log.Infof("[AppendAndRead] %v", b)
		}
	}
}

func GetFileStatus(fs *webhdfs.FileSystem) {
	stat, err := fs.GetFileStatus(fpath)
	if err != nil {
		log.Errorf("%s", err)
		return
	}
	log.Infof("[GetFileStatus] ok: %v", stat)
}

func Truncate(fs *webhdfs.FileSystem) {
	var (
		err error
	)
	for i := 0; i < 5; i++ {
		log.Infof("times %d", i+1)
		err = fs.Truncate(fpath, 3)
		if err == nil {
			log.Infof("times(%d) ok", i+1)
			break
		} else if err != webhdfs.ErrBoolean {
			log.Errorf("%s", err)
			return
		} else {
			log.Infof("times(%d) failed", i+1)
			time.Sleep(1000 * 1000 * 1000 * 1)
		}
	}
	if err != nil {
		log.Errorf("final %s", err)
		return
	}
	if rc, err := fs.Open(fpath, 0, int64(len(BYTE_SRC))); err != nil {
		log.Errorf("%s", err)
		return
	} else {
		b, err := ioutil.ReadAll(rc)
		if err != nil {
			log.Errorf("%s", err)
			return
		} else {
			log.Infof("[TruncateAndRead] %v", b)
		}
	}
}

func ListStatus() {
	fs := &webhdfs.FileSystem{
		NameNodeHost: nameNodeHost,
		NameNodePort: nameNodePort,
		UserName:     hdfsUser,
		BufferSize:   bufferSize,
		BlockSize:    blockSize,
		Replication:  replication,
		Client:       &http.Client{Transport: &http.Transport{Dial: dialTimeout}},
	}
	stat, err := fs.ListStatus(fpath)
	if err != nil {
		log.Errorf("%s", err)
		return
	}
	log.Infof("[ListStatus] ok: %v", stat)
}

func Delete() {
	fs := &webhdfs.FileSystem{
		NameNodeHost: nameNodeHost,
		NameNodePort: nameNodePort,
		UserName:     hdfsUser,
		BufferSize:   bufferSize,
		BlockSize:    blockSize,
		Replication:  replication,
		Client:       &http.Client{Transport: &http.Transport{Dial: dialTimeout}},
	}
	err := fs.Delete(dirpath)
	if err != nil {
		log.Errorf("%s", err)
		if err == webhdfs.ErrBoolean {
			log.Infof("catch boolean err successfully!")
		}
		return
	}
	log.Infof("[Delete] ok")
}

func MkDirs() {
	fs := &webhdfs.FileSystem{
		NameNodeHost: nameNodeHost,
		NameNodePort: nameNodePort,
		UserName:     hdfsUser,
		BufferSize:   bufferSize,
		BlockSize:    blockSize,
		Replication:  replication,
		Client:       &http.Client{Transport: &http.Transport{Dial: dialTimeout}},
	}
	err := fs.MkDirs(dirpath)
	if err != nil {
		log.Errorf("%s", err)
		return
	}
	log.Infof("[MkDirs] ok")
}

func ReadOnly() {
	fs := &webhdfs.FileSystem{
		NameNodeHost: nameNodeHost,
		NameNodePort: nameNodePort,
		UserName:     hdfsUser,
		BufferSize:   bufferSize,
		BlockSize:    blockSize,
		Replication:  replication,
	}
	if rc, err := fs.Open(fpath, 0, 100); err != nil {
		log.Errorf("%s", err)
		return
	} else {
		b, err := ioutil.ReadAll(rc)
		if err != nil {
			log.Errorf("%s", err)
			return
		} else {
			log.Infof("%v", b)
		}
	}
}

func Rename() {
	fs := &webhdfs.FileSystem{
		NameNodeHost: nameNodeHost,
		NameNodePort: nameNodePort,
		UserName:     hdfsUser,
		BufferSize:   bufferSize,
		BlockSize:    blockSize,
		Replication:  replication,
		Client:       &http.Client{Transport: &http.Transport{Dial: dialTimeout}},
	}
	if err := fs.Create(READER2_SRC, f2path); err != nil {
		log.Errorf("%s", err)
		return
	} else {
		log.Infof("Created!")
	}
	if err := fs.MkDirs(path.Dir(fpath)); err != nil {
		log.Errorf("%s", err)
		return
	}
	if err := fs.Rename(f2path, fpath); err != nil {
		log.Errorf("%s", err)
		return
	}

	if rc, err := fs.Open(fpath, 0, int64(len(BYTE2_SRC))); err != nil {
		log.Errorf("%s", err)
		return
	} else {
		b, err := ioutil.ReadAll(rc)
		if err != nil {
			log.Errorf("%s", err)
			return
		} else {
			log.Infof("[CreateAndRead] %v", b)
		}
	}

}

func PrintType() {
	err := webhdfs.RemoteException{}
	name := reflect.TypeOf(err).Name()
	log.Infof("%s", name)
	if name == "RemoteException" {
		log.Infof("Hello World")
	}
}

func dialTimeout(network, addr string) (net.Conn, error) {
	return net.DialTimeout(network, addr, time.Duration(2*time.Second))
}
