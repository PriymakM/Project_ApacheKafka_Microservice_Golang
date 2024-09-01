package handler_producer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	entity_producer "projectKafka/microservice/producer/service/entity"

	"github.com/IBM/sarama"
)

func readUsersFromFile(filename string) ([]entity_producer.User, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var users []entity_producer.User
	err = json.Unmarshal(data, &users)
	if err != nil {
		return nil, err
	}

	return users, nil
}

func HandlerProducer() {
	users, err := readUsersFromFile("users.json")
	if err != nil {
		fmt.Println("Ошибка чтения или парсинга файла: ", err)
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		fmt.Println("Error creating producer! ", err)
	}

	defer producer.Close()

	for _, user := range users {
		message, err := json.Marshal(user)
		if err != nil {
			fmt.Println("Error marshalling user: ", err)
			continue
		}

		msg := &sarama.ProducerMessage{
			Topic: "user_topic",
			Value: sarama.ByteEncoder(message),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			fmt.Println("Error sending date!", err)
		}

		fmt.Printf("Сообщение отправлено на раздел %d с оффсетом %d\n", partition, offset)
	}

}
