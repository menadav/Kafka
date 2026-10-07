package ipc

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

var ErrNoReader = errors.New("no process is reading this FIFO")

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

func CheckEndpoint(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeNamedPipe == 0 {
		return fmt.Errorf("%s is not a FIFO", path)
	}
	return nil
}

func OpenWrite(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if errors.Is(err, syscall.ENXIO) {
		return nil, ErrNoReader
	}
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || info.Mode()&os.ModeNamedPipe == 0 {
		f.Close()
		return nil, fmt.Errorf("%s is not a FIFO", path)
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

func OpenReadNoWait(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
}
