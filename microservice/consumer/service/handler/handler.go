package handler_consumer

import (
	"encoding/json"
	"fmt"
	entity_consumer "projectKafka/microservice/consumer/service/entity"
	"time"

	"github.com/IBM/sarama"
)

func findMaxAge(partitionConsumer sarama.PartitionConsumer, done chan bool) entity_consumer.User {
	var MaxUser entity_consumer.User
	var firstMessageReceived bool

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			if !firstMessageReceived {
				firstMessageReceived = true
				go func() {
					time.Sleep(4 * time.Second)
					done <- true
				}()
			}

			var user entity_consumer.User
			err := json.Unmarshal(msg.Value, &user)
			if err != nil {
				fmt.Println("Error unmarshalling message!", err)
				continue
			}

			if user.Age > MaxUser.Age {
				MaxUser = user
			}

			fmt.Println("Received message: ", string(msg.Value))

		case <-done:
			fmt.Println("Stopping message processing")
			return MaxUser
		}
	}
}

func HandlerConsumer() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		fmt.Println("Error creating consumer!", err)
		return
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("user_topic", 0, sarama.OffsetNewest)
	if err != nil {
		fmt.Println("Error consuming partition!", err)
		return
	}
	defer partitionConsumer.Close()

	done := make(chan bool)

	maxAgeUser := findMaxAge(partitionConsumer, done)
	fmt.Println("User max age: ", maxAgeUser)
}
