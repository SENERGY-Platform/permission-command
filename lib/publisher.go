package lib

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/segmentio/kafka-go"
	"io/ioutil"
	"log"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"time"
)



type Publisher struct {
	writer *kafka.Writer
}

func NewPublisher() (*Publisher, error) {
	err := InitTopic(Config.KafkaUrl, Config.PermTopic)
	if err != nil {
		return nil, err
	}
	broker, err := GetBroker(Config.KafkaUrl)
	if err != nil {
		return nil, err
	}
	if len(broker) == 0 {
		return nil, errors.New("missing kafka broker")
	}
	writer, err := GetKafkaWriter(broker, Config.PermTopic, Config.LogLevel == "DEBUG")
	if err != nil {
		return nil, err
	}
	return &Publisher{writer: writer}, nil
}

func (this *Publisher) Publish(command PermCommandMsg) (err error) {
	message, err := json.Marshal(command)
	if err != nil {
		return err
	}
	err = this.writer.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte(command.Resource+"_"+command.User+"_"+command.Group),
			Value: message,
			Time:  time.Now(),
		},
	)
	if err != nil {
		debug.PrintStack()
	}
	return err
}

func GetKafkaWriter(broker []string, topic string, debug bool) (writer *kafka.Writer, err error) {
	var logger *log.Logger
	if debug {
		logger = log.New(os.Stdout, "[KAFKA-PRODUCER] ", 0)
	} else {
		logger = log.New(ioutil.Discard, "", 0)
	}
	writer = &kafka.Writer{
		Addr:        kafka.TCP(broker...),
		Topic:       topic,
		MaxAttempts: 10,
		Logger:      logger,
	}
	return writer, err
}

func GetBroker(bootstrapUrl string) (brokers []string, err error) {
	return getBroker(bootstrapUrl)
}

func getBroker(bootstrapUrl string) (result []string, err error) {
	conn, err := kafka.Dial("tcp", bootstrapUrl)
	if err != nil {
		return result, err
	}
	defer conn.Close()
	brokers, err := conn.Brokers()
	if err != nil {
		return result, err
	}
	for _, broker := range brokers {
		result = append(result, net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)))
	}
	return result, nil
}

func InitTopic(bootstrapUrl string, topics ...string) (err error) {
	conn, err := kafka.Dial("tcp", bootstrapUrl)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{}

	for _, topic := range topics {
		topicConfigs = append(topicConfigs, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "retention.ms",
					ConfigValue: "-1",
				},
				{
					ConfigName:  "retention.bytes",
					ConfigValue: "-1",
				},
				{
					ConfigName:  "cleanup.policy",
					ConfigValue: "compact",
				},
				{
					ConfigName:  "delete.retention.ms",
					ConfigValue: "86400000",
				},
				{
					ConfigName:  "segment.ms",
					ConfigValue: "604800000",
				},
				{
					ConfigName:  "min.cleanable.dirty.ratio",
					ConfigValue: "0.1",
				},
			},
		})
	}

	return controllerConn.CreateTopics(topicConfigs...)
}

