package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	pulsarLog "github.com/apache/pulsar-client-go/pulsar/log"
)

type (
	Config struct {
		PulsarURI string
		Topic     string
	}
)

func main() {
	var config Config
	pulsarURI, isPulsarURISet := os.LookupEnv("PULSAR_URI")
	if !isPulsarURISet {
		log.Fatal("Pulsar URI not set")
	}

	topic, isTopicSet := os.LookupEnv("TOPIC")
	if !isTopicSet {
		log.Fatal("Pulsar Topic not set")
	}

	config.PulsarURI = pulsarURI
	config.Topic = topic

	var client pulsar.Client
	var clientErr error

	// Open Pulsar cluster
	client, clientErr = pulsar.NewClient(pulsar.ClientOptions{
		URL:               config.PulsarURI,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
		Logger:            pulsarLog.DefaultNopLogger(),
	})

	if clientErr != nil {
		log.Fatal("Error creating client - TERMINATING", clientErr)
	}

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: config.Topic,
	})
	if err != nil {
		log.Fatal("Error creating producer - TERMINATING", err)
	}

	defer producer.Close()

	log.Println("Sending messages")
	for i := 0; i < 50; i++ {
		producer.SendAsync(
			context.Background(),
			&pulsar.ProducerMessage{
				Payload: []byte(fmt.Sprintf("{\"name\":\"cgiacomi\",\"timestamp\":%d}", time.Now().Unix())),
			},
			func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
				if err != nil {
					log.Printf("error producing message: %v\n", err)
				}

				fmt.Printf("successfully produced message: %s\n", message.Payload)
			})
	}

	err = producer.Flush()
	if err != nil {
		log.Fatal("Error flushing producer")
	}
}
