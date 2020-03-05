package lib


import (
	"context"
	"encoding/json"
	"errors"
	"github.com/segmentio/kafka-go"
	"github.com/wvanbergen/kazoo-go"
	"io/ioutil"
	"log"
	"os"
	"runtime/debug"
	"time"
)



type Publisher struct {
	writer *kafka.Writer
}

func NewPublisher() (*Publisher, error) {
	err := InitTopic(Config.ZookeeperUrl, Config.PermTopic)
	if err != nil {
		return nil, err
	}
	broker, err := GetBroker(Config.ZookeeperUrl)
	if err != nil {
		return nil, err
	}
	if len(broker) == 0 {
		return nil, errors.New("missing kafka broker")
	}
	writer, err := getProducer(broker, Config.PermTopic, Config.LogLevel == "DEBUG")
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
			Key:   []byte(command.Kind+"_"+command.Resource),
			Value: message,
			Time:  time.Now(),
		},
	)
	if err != nil {
		debug.PrintStack()
	}
	return err
}

func getProducer(broker []string, topic string, debug bool) (writer *kafka.Writer, err error) {
	var logger *log.Logger
	if debug {
		logger = log.New(os.Stdout, "[KAFKA-PRODUCER] ", 0)
	} else {
		logger = log.New(ioutil.Discard, "", 0)
	}
	writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:     broker,
		Topic:       topic,
		MaxAttempts: 10,
		Logger:      logger,
	})
	return writer, err
}


func GetBroker(zk string) (brokers []string, err error) {
	return getBroker(zk)
}

func getBroker(zkUrl string) (brokers []string, err error) {
	zookeeper := kazoo.NewConfig()
	zookeeper.Logger = log.New(ioutil.Discard, "", 0)
	zk, chroot := kazoo.ParseConnectionString(zkUrl)
	zookeeper.Chroot = chroot
	if kz, err := kazoo.NewKazoo(zk, zookeeper); err != nil {
		return brokers, err
	} else {
		return kz.BrokerList()
	}
}

func GetKafkaController(zkUrl string) (controller string, err error) {
	zookeeper := kazoo.NewConfig()
	zookeeper.Logger = log.New(ioutil.Discard, "", 0)
	zk, chroot := kazoo.ParseConnectionString(zkUrl)
	zookeeper.Chroot = chroot
	kz, err := kazoo.NewKazoo(zk, zookeeper)
	if err != nil {
		return controller, err
	}
	controllerId, err := kz.Controller()
	if err != nil {
		return controller, err
	}
	brokers, err := kz.Brokers()
	if err != nil {
		return controller, err
	}
	return brokers[controllerId], err
}

func InitTopic(zkUrl string, topics ...string) (err error) {
	return InitTopicWithConfig(zkUrl, 1, 1, topics...)
}

func InitTopicWithConfig(zkUrl string, numPartitions int, replicationFactor int, topics ...string) (err error) {
	controller, err := GetKafkaController(zkUrl)
	if err != nil {
		log.Println("ERROR: unable to find controller", err)
		return err
	}
	if controller == "" {
		log.Println("ERROR: unable to find controller")
		return errors.New("unable to find controller")
	}
	initConn, err := kafka.Dial("tcp", controller)
	if err != nil {
		log.Println("ERROR: while init topic connection ", err)
		return err
	}
	defer initConn.Close()
	for _, topic := range topics {
		err = initConn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
			ConfigEntries: []kafka.ConfigEntry{
				{ConfigName: "retention.ms", ConfigValue: "-1"},
				{ConfigName: "retention.bytes", ConfigValue: "-1"},
				{ConfigName: "cleanup.policy", ConfigValue: "compact"},
				{ConfigName: "delete.retention.ms", ConfigValue: "86400000"},
				{ConfigName: "segment.ms", ConfigValue: "604800000"},
				{ConfigName: "min.cleanable.dirty.ratio", ConfigValue: "0.1"},
			},
		})
		if err != nil {
			return
		}
	}
	return nil
}
