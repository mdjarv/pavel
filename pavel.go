package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"bufio"
	"os/signal"
	"syscall"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app = kingpin.New("pavel", "A command-line tool to consume or produce Kafka messages.")

	importCommand = app.Command("produce", "Send file contents as line by line messages to kafka.")
	importBroker  = importCommand.Arg("broker", "Kafka broker to connect to.").Required().String()
	importTopic   = importCommand.Arg("topic", "Kafka topic.").Required().String()
	importFile    = importCommand.Arg("file", "File to import.").Required().String()

	exportCommand = app.Command("consume", "Read messages from a Kafka topic.")
	exportListen  = exportCommand.Flag("listen", "Continuously listen to kafka topic").Default("false").Bool()
	exportOffset  = exportCommand.Flag("offset", "Offset to start from").Default("earliest").String()
	exportBroker  = exportCommand.Arg("broker", "Kafka broker to connect to.").Required().String()
	exportTopic   = exportCommand.Arg("topic", "Kafka topic.").Required().String()
	exportFile    = exportCommand.Arg("file", "File to import.").Default("/dev/stdout").String()
)

func main() {
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case importCommand.FullCommand():
		importToKafka(*importBroker, *importTopic, *importFile)

	case exportCommand.FullCommand():
		exportFromKafka(*exportBroker, *exportTopic, *exportFile, *exportListen, *exportOffset)
	}
}

func importToKafka(broker string, topic string, input string) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created producer %v\n", p)

	file, err := os.Open(input)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		doneChan := make(chan bool)

		go func() {
			defer close(doneChan)
			for e := range p.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					m := ev
					if m.TopicPartition.Error != nil {
						fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
					} else {
						fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
							*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}
					return
	
				default:
					fmt.Printf("Ignored event: %s\n", ev)
				}
			}
		}()
	
		p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(scanner.Text())}
	
		// wait for delivery report goroutine to finish
		_ = <-doneChan
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
	p.Close()
}

func exportFromKafka(broker string, topic string, outputFileName string, listen bool, startOffset string) {
	if listen {
		fmt.Fprintf(os.Stderr, "Listening to %s/%s\n", broker, topic)
	} else {
		fmt.Fprintf(os.Stderr, "Consuming messages from %s/%s\n", broker, topic)
	}

	f, err := os.OpenFile(outputFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
		panic(err)
	}
    defer f.Close()

	output := bufio.NewWriter(f)
	
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        "pavel",
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": kafka.OffsetBeginning.String()}})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	//fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics([]string{topic}, nil)

	run := true

	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Fprintf(os.Stderr, "Caught signal %v: terminating\n", sig)
			run = false

		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				//fmt.Fprintf(os.Stderr, "%v\n", e)
				var parts []kafka.TopicPartition

				for _, part := range(e.Partitions) {
					offset, err := kafka.NewOffset(startOffset)
					if err == nil {
						part.Offset = offset
					}
					parts = append(parts, part)
				}
				//fmt.Printf("  %+v\n", parts)
				c.Assign(parts)
			case kafka.RevokedPartitions:
				//fmt.Fprintf(os.Stderr, "%v\n", e)
				c.Unassign()
			case *kafka.Message:
				//fmt.Printf("%% Message on %s:\n%s\n",
				//	e.TopicPartition, string(e.Value))
				fmt.Fprintf(output, "%s\n", string(e.Value))
				output.Flush()
			case kafka.PartitionEOF:
				//fmt.Printf("Reached the end at %v\n", e)
				if listen == false {
					run = false
				}
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "Error: %v\n", e)
				run = false
			}
		}
	}
	c.Close()
}