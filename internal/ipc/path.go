package ipc

import (
	"fmt"
	"os"
	"path/filepath"
)

const namePrefix = "kafka.server"

func BuildIPCPath(pid int) string {
	dir := os.TempDir()
	name := fmt.Sprintf(namePrefix+".%d", pid)
	return filepath.Join(dir, name)
}

func ChildPath(base string, suffix string) string {
	return base + "." + suffix
}
