package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

// Client — клиент Kafka.
type Client struct {
	// Reader осуществляет операции чтения топика.
	Reader *kafka.Reader

	// Writer осуществляет операции записи в топики.
	Writer *kafka.Writer
}

// New создаёт и инициализирует клиента Kafka.
// Функция-конструктор.
func New(brokers []string, topic string, groupId string) (*Client, error) {
	if len(brokers) == 0 || brokers[0] == "" || topic == "" || groupId == "" {
		return nil, errors.New("не указаны параметры подключения к Kafka")
	}

	c := Client{}

	// Инициализация компонента получения сообщений.
	c.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupId,
		MinBytes: 10e1,
		MaxBytes: 10e6,
	})

	// Инициализация компонента отправки сообщений.
	c.Writer = &kafka.Writer{
		Addr:     kafka.TCP(brokers[0]),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	return &c, nil
}

func main() {
	brokers := []string{"localhost:9092"}
	topic := "my-topic"
	groupID := "my-group"

	// Инициализация клиента Kafka.
	kfk, err := New(brokers, topic, groupID)
	if err != nil {
		log.Fatal(err)
	}

	// Отправка сообщения.
	go func() {
		for range time.Tick(time.Second) {
			// Массив сообщений для отправки.
			messages := []kafka.Message{
				{
					Key:   []byte("Сообщение : "),
					Value: []byte(time.Now().Format(time.RFC3339)),
				},
			}
			err := kfk.sendMessages(messages)
			if err != nil {
				fmt.Println(err)
			}
		}
	}()

	// чтение следующего сообщения.
	go func() {
		for {
			err := kfk.fetchProcessCommit()
			if err != nil {
				fmt.Println(err)
			}
		}
	}()

	// Ожидаем завершения работы программы.
	select {}

}

// sendMessages отправляет сообщения в Kafka.
func (c *Client) sendMessages(messages []kafka.Message) error {
	err := c.Writer.WriteMessages(context.Background(), messages...)
	if err != nil {
		fmt.Printf("Ошибка отправки сообщения: %v\n", err)
	}

	return err
}

// fetchProcessCommit сначала выбирает сообщение из очереди,
// потом обрабатывает, после чего подтверждает.
func (c *Client) fetchProcessCommit() error {
	// Выборка очередного сообщения из Kafka.
	for {
		msg, err := c.Reader.FetchMessage(context.Background())
		if err != nil {
			return err
		}

		// Обработка сообщения
		fmt.Printf("%s%s\n", msg.Key, msg.Value)

		// Подтверждение сообщения как обработанного.
		err = c.Reader.CommitMessages(context.Background(), msg)
		return err
	}
}
