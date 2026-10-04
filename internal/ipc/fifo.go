package ipc

import (
	"os"
	"syscall"
)

func CreateFIFO(path string) error {
	err := syscall.Mkfifo(path, 0666)
	if err != nil {
		if os.IsExist(err) {
			os.Remove(path)
			err = syscall.Mkfifo(path, 0666)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func OpenWrite(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func OpenRead(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func OpenEndpoint(path string) (*os.File, error) {
	endpoint, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	return endpoint, nil
}
