package main

import(
    "os"
    "fmt"
    "io"
    "syscall"
    "kafka/internal/ipc"
)

func main(){

    if len(os.Args) != 2{
        fmt.Fprintln(os.Stderr, "invalid arguments")
        os.Exit(1)
    }
    serverPath := os.Args[1]
    replyPath := ipc.ChildPath(serverPath, fmt.Sprintf("ping.%d", os.Getpid()))
    err := syscall.Mkfifo(replyPath, 0666)
    if err != nil {
        if os.IsExist(err) {
            os.Remove(replyPath)
            err = syscall.Mkfifo(replyPath, 0666)
        }
        if err != nil {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }
    }
    endpoint, err := os.OpenFile(serverPath, os.O_WRONLY, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
    message := fmt.Sprintf("PING %s\n", replyPath)
    endpoint.WriteString(message)
    endpoint.Close()
    reply, err := os.OpenFile(replyPath, os.O_RDONLY, 0)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    data, err := io.ReadAll(reply)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    fmt.Print(string(data))
    reply.Close()
    os.Remove(replyPath)
}