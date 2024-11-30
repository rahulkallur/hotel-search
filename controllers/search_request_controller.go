package controllers

import (
	model "Search/models"
	services "Search/services"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

var (
	producer      sarama.SyncProducer
	listeners     = make(map[string]chan interface{})
	listenersLock sync.Mutex
)

type SearchRequestController interface {
	SearchRequestMapper(ctx *gin.Context)
}

type searchRequestcontroller struct {
	repo services.SearchRepository
}

func NewSearchRequestController(repo services.SearchRepository) SearchRequestController {
	return &searchRequestcontroller{
		repo: repo,
	}
}

func initProducer() sarama.SyncProducer {
	if os.Getenv("ENV") == "development" {
		if err := godotenv.Load(); err != nil {
			log.Println("Error loading .env file, but it's not required in production.")
		}
	}

	fmt.Println(os.Getenv("KAFKA_BROKER_URL"))
	// Read broker URL(s) from an environment variable
	brokerURL := os.Getenv("KAFKA_BROKER_URL")
	if brokerURL == "" {
		log.Fatal("KAFKA_BROKER_URL environment variable is not set")
	}

	// Split broker URLs into a slice (in case there are multiple brokers)
	brokers := strings.Split(brokerURL, ",")

	// Configure Sarama
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Net.SASL.Enable = true
	config.Net.SASL.User = os.Getenv("KAFKA_USERNAME")
	config.Net.SASL.Password = os.Getenv("KAFKA_PASSWORD")

	// Create a new producer
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}

	log.Printf("Kafka producer initialized successfully with brokers: %v", brokers)
	return producer
}

func publishMessage(topic, key string, message []byte) error {
	if producer == nil {
		log.Println("Producer is not initialized")
		return fmt.Errorf("producer is nil")
	}

	if topic == "" {
		log.Println("Topic is empty")
		return fmt.Errorf("topic is empty")
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(message),
	}
	_, _, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("Failed to send message to topic %s: %v", topic, err)
		return err
	}
	log.Printf("Message successfully sent to topic %s", topic)
	return nil
}

func init() {
	producer = initProducer()
}

// SearchRequestMapper handles incoming hotel search requests.
func (c *searchRequestcontroller) SearchRequestMapper(ctx *gin.Context) {
	currentTime := time.Now().UnixNano() / int64(time.Millisecond)
	log.Printf("Search request received at: %d", currentTime)
	rq := model.HotelSearchRequest{}
	if err := ctx.BindJSON(&rq); err != nil {
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}
	// Generate correlationId
	correlationID := uuid.New().String()
	// log.Printf("Registering listener for correlationId: %s", correlationID)

	// Create a channel for the listener
	responseChan := make(chan interface{}) // Buffered channel
	listenersLock.Lock()
	listeners[correlationID] = responseChan
	listenersLock.Unlock()

	defer func() {
		// Remove listener after response is received or timeout
		listenersLock.Lock()
		delete(listeners, correlationID)
		listenersLock.Unlock()
		// log.Printf("Listener removed for correlationId: %s", correlationID)
	}()

	// Pass the mapped request to the repository for further processing.
	request := c.repo.SearchRequestMapper(rq)
	// log.Printf("Search request: %s", request)

	requestData := map[string]string{
		"data":          request,
		"correlationId": correlationID,
	}

	payload, _ := json.Marshal(requestData)

	// Publish the message
	err := publishMessage(os.Getenv("PUBLISH_TOPIC"), correlationID, payload)
	if err != nil {
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}

	// Wait for a response
	response, err := waitForResponse(correlationID)
	if err != nil {
		ctx.JSON(http.StatusRequestTimeout, gin.H{"error": "Timeout waiting for response"})
		return
	}
	responseTime := time.Now().UnixNano() / int64(time.Millisecond)
	log.Printf("Response received at: %d", responseTime)
	log.Printf("Total time: %d", responseTime-currentTime)

	// Send the response back to the client
	ctx.JSON(http.StatusOK, response)
}

func shutdownProducer() {
	if producer != nil {
		_ = producer.Close()
	}
}

func waitForResponse(correlationID string) (map[string]interface{}, error) {
	// log.Printf("Waiting for response for correlationId: %s", correlationID)

	// Start listening for responses only once

	// Create a new response channel for the current correlationId
	responseChan := make(chan interface{})

	// log.Printf("Response channel created for correlationId: %s", correlationID)

	// Register the listener for the current correlationId
	listenersLock.Lock()
	listeners[correlationID] = responseChan
	listenersLock.Unlock()

	defer func() {
		// Clean up the listener when done
		listenersLock.Lock()
		delete(listeners, correlationID)
		listenersLock.Unlock()
		log.Printf("Listener removed for correlationId: %s", correlationID)
	}()

	// Wait for response or timeout (30 seconds)
	select {
	case response := <-responseChan:
		if parsedResponse, ok := response.(map[string]interface{}); ok {
			// log.Printf("Received parsed response for correlationId: %s", correlationID)
			return parsedResponse, nil
		}
		return nil, fmt.Errorf("unexpected response type")
	case <-time.After(30 * time.Second):
		log.Printf("Timeout waiting for response for correlationId: %s", correlationID)
		return nil, fmt.Errorf("timeout waiting for response")
	}
}

func StartConsumer() {
	// Ensure consumer starts once at the start of the app
	brokers := []string{os.Getenv("KAFKA_BROKER_URL")}
	log.Println("Starting Kafka consumer...")

	currentTime := time.Now().UnixNano() / int64(time.Millisecond)
	consumerGrp := "search-hotel-consumer" + strconv.FormatInt(currentTime, 10)

	// Create Kafka consumer group
	consumerGroup, err := sarama.NewConsumerGroup(brokers, consumerGrp, nil)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	// Start consuming messages from the subscribed topic
	go func() {
		for {
			err := consumerGroup.Consume(context.Background(), []string{os.Getenv("SUBSCRIBE_TOPIC")}, consumerGroupHandler{})
			if err != nil {
				log.Printf("Error consuming messages: %v", err)
				continue
			}
		}
	}()
}

func listenForResponses() {
	// Ensure the consumer is set up and starts consuming messages
	brokers := []string{os.Getenv("KAFKA_BROKER_URL")}
	log.Println("Listening for responses...")

	consumerGroup, err := sarama.NewConsumerGroup(brokers, "search-hotel-consumer", nil)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	// Start consuming messages
	for {
		err := consumerGroup.Consume(context.Background(), []string{os.Getenv("SUBSCRIBE_TOPIC")}, consumerGroupHandler{})
		if err != nil {
			log.Printf("Error consuming messages: %v", err)
			continue
		}
	}
}

// Consumer group handler to handle messages from Kafka
type consumerGroupHandler struct{}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// log.Printf("Message received: key=%s, value=%s, topic=%s", string(message.Key), string(message.Value), message.Topic)

		messageData := string(message.Value)
		// Unmarshal the response data
		var responseData map[string]interface{}
		err := json.Unmarshal([]byte(messageData), &responseData)
		if err != nil {
			log.Printf("Failed to parse message: %v", err)
			continue
		}

		// Combine hotel_listing and tracker_id
		responseObject := map[string]interface{}{
			"tracker_id":    responseData["tracker_id"],
			"hotel_listing": responseData["hotel_listing"],
		}

		// fmt.Println("Response data: ", responseObject)

		correlationID := string(message.Key)
		// log.Printf("Checking listener map for correlationId: %s", correlationID)

		// Check if a listener exists for the given correlationId
		listenersLock.Lock()
		responseChan, exists := listeners[correlationID]
		listenersLock.Unlock()

		if exists {
			// log.Printf("Found listener for correlationId: %s", correlationID)

			// responseJSON, err := json.Marshal(responseObject)
			// if err != nil {
			// 	log.Printf("Failed to marshal response object: %v", err)
			// 	continue
			// }
			// Send the response to the listener's channel
			select {
			case responseChan <- responseObject:
				// log.Printf("Response sent to listener for correlationId: %s", correlationID)
			default:
				// log.Printf("Listener for correlationId %s is not ready to receive data", correlationID)
			}
		} else {
			log.Printf("No listener found for correlationId: %s", correlationID)
		}

		// Mark the message as processed
		session.MarkMessage(message, "")
	}
	return nil
}
