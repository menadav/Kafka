package main

import(
    "os"
    "fmt"
    "io"
    "kafka/internal/ipc"
)

func main(){

    if len(os.Args) != 2{
        fmt.Fprintln(os.Stderr, "invalid arguments")
        os.Exit(1)
    }
    serverPath := os.Args[1]
    replyPath := ipc.ChildPath(serverPath, fmt.Sprintf("ping.%d", os.Getpid()))
	err := ipc.CreateFIFO(replyPath)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
	endpoint, err := ipc.OpenWrite(serverPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
    message := fmt.Sprintf("PING %s\n", replyPath)
    endpoint.WriteString(message)
    endpoint.Close()
    reply, err := ipc.OpenRead(replyPath)
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