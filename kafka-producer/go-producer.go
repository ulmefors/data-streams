package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ulmefors/data-schemas/gen-go/state"
	"google.golang.org/protobuf/proto"
)

const (
	kafkaConn = "localhost:9092"
	topic     = "state-metrics"
)

func main() {
	producer, err := initProducer()
	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		os.Exit(1)
	}

	r := rand.New(rand.NewSource(42))

	for {
		now := time.Now().UnixNano()
		temperature := 25.0 + 10.0*r.Float32()

		stateMetrics := state.StateMetrics{
			Time:        &now,
			Temperature: &temperature,
		}
		bytes, err := proto.Marshal(&stateMetrics)
		if err != nil {
			fmt.Println("Error Marshal: ", err.Error())
		}
		publish(bytes, producer)
		time.Sleep(1 * time.Second)
	}
}

func publish(bytes []byte, producer sarama.SyncProducer) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(bytes),
	}
	p, o, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error publish: ", err.Error())
	}
	fmt.Println("p, o", p, o, msg)
}

func initProducer() (sarama.SyncProducer, error) {
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	prd, err := sarama.NewSyncProducer([]string{kafkaConn}, config)
	return prd, err
}
