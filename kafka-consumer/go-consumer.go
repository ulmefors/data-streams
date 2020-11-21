package main

import (
	"flag"
	"fmt"
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
	offset := flag.Int64("offset", 0, "Offset to start consuming")
	partition := flag.Int("partition", 0, "Partition to consume")
	flag.Parse()

	partitionConsumer, err := initPartitionConsumer(topic, int32(*partition), *offset)
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
				fmt.Println("Error Unmarshal: ", err)
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
