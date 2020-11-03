# data-streams

## Install

```bash
go get -u github.com/ulmefors/data-schemas/gen-go/state
```

## Run

```bash
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties
```

```bash
go run kafka-producer/go-producer.go
```

```bash
go run kafka-producer/go-consumer.go
```
