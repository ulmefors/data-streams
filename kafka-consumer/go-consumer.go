package main

import (
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ulmefors/data-schemas/gen-go/state"
	"google.golang.org/protobuf/proto"
)

const (
	kafkaConn    = "localhost:9092"
	topic        = "state-metrics"
	start_offset = 0
	partition    = 0
)

func main() {
	partitionConsumer, err := initPartitionConsumer(topic, partition, start_offset)
	if err != nil {
		fmt.Println("Error initPartitionConsumer: ", err)
		os.Exit(1)
	}
	defer partitionConsumer.Close()

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			bytes := msg.Value
			stateMetrics := &state.StateMetrics{}
			if err := proto.Unmarshal(bytes, stateMetrics); err != nil {
				fmt.Println("Error unmarshal: ", err)
			}
			fmt.Println(stateMetrics, time.Unix(int64(stateMetrics.GetTime()/1e9), 0))
		}
	}
}

func initPartitionConsumer(topic string, partition int32, start_offset int64) (sarama.PartitionConsumer, error) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer([]string{kafkaConn}, config)
	if err != nil {
		fmt.Println("Error new consumer: ", err)
		return nil, err
	}
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, start_offset)
	if err != nil {
		fmt.Println("Error partition consumer: ", err)
		return nil, err
	}
	return partitionConsumer, nil
}
