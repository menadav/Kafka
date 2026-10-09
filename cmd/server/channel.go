package main

import (
	"os"
	"time"

	"kafka/internal/broker"
	"kafka/internal/protocol"
)

const writeTimeout = 2 * time.Second

type consumerChannel struct {
	f *os.File
}

func (c consumerChannel) Send(m broker.Message) error {
	c.f.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := c.f.WriteString(protocol.EncodeDelivery(m.Offset, m.Key, m.Value))
	return err
}

func (c consumerChannel) Goodbye() error {
	c.f.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := c.f.WriteString(protocol.Goodbye + "\n")
	return err
}

func (c consumerChannel) Close() error {
	return c.f.Close()
}
