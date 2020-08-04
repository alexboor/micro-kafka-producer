package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	//"micro-kafka-producer/cmd"
	"github.com/Shopify/sarama"
	"log"
	"os"
)

const (
	CLIENTID       = "micro-kafka-producer"
	KAFKAADDRESSES = "127.0.0.1:29092"
	TOPIC          = "in-example"
	FILENAME       = "message.json"
)

func main() {
	//cmd.Execute()

	msg, err := getJSONFromFile(FILENAME)
	if err != nil {
		fmt.Println("error to read file: ", err)
		os.Exit(1)
	}

	producer, err := initProducer()
	if err != nil {
		fmt.Println("error init producer: ", err)
		os.Exit(1)
	}

	if err := pub(msg, producer); err != nil {
		fmt.Println("error publish message: ", err)
	}

}

// Read given file and parse content into interface{} struct
// Suppose that the file contain valid json object
func getJSONFromFile(fn string) (interface{}, error) {
	var r interface{}

	file, err := ioutil.ReadFile(fn)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal([]byte(file), &r); err != nil {
		return nil, err
	}

	return r, nil
}

// Create initial configuration for the sarama producer
func initProducer() (sarama.SyncProducer, error) {
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	kafkaVersion, err := sarama.ParseKafkaVersion("1.0.0")
	if err != nil {
		return nil, err
	}

	conf := sarama.NewConfig()
	conf.Producer.Retry.Max = 5
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Successes = true
	conf.Producer.Compression = sarama.CompressionNone
	conf.Version = kafkaVersion
	conf.ClientID = CLIENTID

	return sarama.NewSyncProducer([]string{KAFKAADDRESSES}, conf)
}

// Publish given message with given producer
func pub(m interface{}, producer sarama.SyncProducer) error {

	jsonMsg, err := json.Marshal(m)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: TOPIC,
		Key:   sarama.StringEncoder("data"),
		Value: sarama.ByteEncoder(jsonMsg),
	}

	p, o, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("published OK! Partition: %d, Offset: %d", p, o)

	return nil
}
