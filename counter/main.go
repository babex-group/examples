package main

import (
	"encoding/json"
	"fmt"
	"github.com/babex-group/babex"
	"github.com/babex-group/babex-kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	adapter, err := kafka.NewAdapter(kafka.Options{
		Name:   "sum-service",
		Addrs:  []string{"localhost:29092"},
		Topics: []string{"sum"},
	})
	if err != nil {
		log.Fatal(err)
	}

	service := babex.NewService(adapter)

	defer service.Close()

	go func() {
		for err := range service.GetErrors() {
			fmt.Printf("receive error: %s\r\n", err)
		}
	}()

	service.Handler("sum", "", func(msg *babex.Message) error {
		var data struct {
			Count int `json:"count"`
		}

		if err := json.Unmarshal(msg.Data, &data); err != nil {
			return service.Catch(msg, err, nil)
		}

		data.Count += 1

		return service.Next(msg, data, nil)
	})

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	<-signals
}
