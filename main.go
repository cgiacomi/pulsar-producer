package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	pulsarLog "github.com/apache/pulsar-client-go/pulsar/log"
)

type (
	Config struct {
		ClientID     string
		ClientSecret string
		IssuerURL    string
		Scope        string
		Audience     string
		PulsarURI    string
		Topic        string
	}
)

func main() {
	var config Config
	clientID, isClientIDSet := os.LookupEnv("CLIENT_ID")
	if isClientIDSet {
		config.ClientID = clientID
	}

	clientSecret, isClientSecretSet := os.LookupEnv("CLIENT_SECRET")
	if isClientSecretSet {
		config.ClientSecret = clientSecret
	}

	issuerURL, isIssuerURLSet := os.LookupEnv("ISSUER_URL")
	if isIssuerURLSet {
		config.IssuerURL = issuerURL
	}

	if scope, isScopeSet := os.LookupEnv("SCOPE"); isScopeSet {
		config.Scope = scope
	}

	if audience, isAudienceSet := os.LookupEnv("AUDIENCE"); isAudienceSet {
		config.Audience = audience
	}

	if pulsarURI, isPulsarURISet := os.LookupEnv("PULSAR_URI"); isPulsarURISet {
		config.PulsarURI = pulsarURI
	}

	if topic, isTopicSet := os.LookupEnv("TOPIC"); isTopicSet {
		config.Topic = topic
	}

	var pulsarKeyFile []byte
	var client pulsar.Client
	var clientErr error

	if isClientIDSet && isClientSecretSet {
		// If we are producing to an Pulsar that implements OAuth2 auth
		var err error
		pulsarKeyFile, err = json.Marshal(map[string]string{
			"type":          "client_credentials",
			"client_id":     config.ClientID,
			"client_secret": config.ClientSecret,
			"issuer_url":    config.IssuerURL,
			"scope":         config.Scope,
		})
		if err != nil {
			log.Fatal(err)
		}

		client, clientErr = pulsar.NewClient(pulsar.ClientOptions{
			URL:               config.PulsarURI,
			OperationTimeout:  30 * time.Second,
			ConnectionTimeout: 30 * time.Second,
			Logger:            pulsarLog.DefaultNopLogger(),
			Authentication: pulsar.NewAuthenticationOAuth2(map[string]string{
				"type":       "client_credentials",
				"issuerUrl":  config.IssuerURL,
				"clientId":   config.ClientID,
				"audience":   config.Audience,
				"scope":      config.Scope,
				"privateKey": fmt.Sprintf("data://%s", pulsarKeyFile),
			}),
		})

	} else {
		// Open Pulsar cluster
		client, clientErr = pulsar.NewClient(pulsar.ClientOptions{
			URL:               config.PulsarURI,
			OperationTimeout:  30 * time.Second,
			ConnectionTimeout: 30 * time.Second,
			Logger:            pulsarLog.DefaultNopLogger(),
		})
	}

	if clientErr != nil {
		log.Fatal(clientErr)
	}

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: config.Topic,
	})
	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	for i := 0; i < 50; i++ {
		producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			Value: fmt.Sprintf("Test Message: %d", i),
		}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			if err != nil {
				log.Printf("error producing message: %v\n", err)
			}
			log.Println("successfully produced message")
		})
	}
}
