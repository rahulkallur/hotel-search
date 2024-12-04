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
	listeners     = make(map[string]*listenerData)
	listenersLock sync.Mutex
)

type listenerData struct {
	counter    int
	batchCount int
	responses  []map[string]interface{}
	responseCh chan interface{}
}

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
		err := godotenv.Load()
		if err != nil {
			log.Printf("Error loading .env file: %v", err)
		} else {
			log.Println("Successfully loaded .env file")
		}
	}
	// Read broker URL(s) from an environment variable
	brokerURL := os.Getenv("KAFKA_BROKER_URL")
	if brokerURL == "" {
		log.Fatal("KAFKA_BROKER_URL environment variable is not set")
	}

	// Split broker URLs into a slice (in case there are multiple brokers)
	brokers := strings.Split(brokerURL, ",")
	fmt.Println(brokers)

	// Configure Sarama
	config := sarama.NewConfig()
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Net.SASL.Enable = true

	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	config.Net.SASL.User = os.Getenv("KAFKA_USERNAME")
	config.Net.SASL.Password = os.Getenv("KAFKA_PASSWORD")
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512

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
	listeners[correlationID] = &listenerData{
		counter:    0,
		batchCount: 0, // Will be updated with the number of batches
		responses:  []map[string]interface{}{},
		responseCh: responseChan,
	}
	listenersLock.Unlock()

	defer func() {
		// Remove listener after response is received or timeout
		listenersLock.Lock()
		delete(listeners, correlationID)
		listenersLock.Unlock()
		// log.Printf("Listener removed for correlationId: %s", correlationID)
	}()

	hotelCodes := extractHotelCodes(rq)
	if hotelCodes == nil {
		log.Printf("Hotel codes missing or invalid")
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}

	batchSizeStr := os.Getenv("BATCH_SIZE")

	// Convert the string to an integer
	batchSize, err := strconv.Atoi(batchSizeStr)
	if err != nil {
		fmt.Println("Error converting BATCH_SIZE:", err)
		return
	}

	batches := splitHotelCodesIntoBatches(hotelCodes, batchSize)

	listenersLock.Lock()
	listeners[correlationID].batchCount = len(batches)
	listenersLock.Unlock()

	fmt.Println("Number of batches:", len(batches))
	for _, batch := range batches {
		rq.HotelCode = strings.Join(batch, ",")
		request := c.repo.SearchRequestMapper(rq)

		fmt.Println("Request Mapper for correlationId: %s", request)
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
	}
	// Pass the mapped request to the repository for further processing.

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

func extractHotelCodes(rq model.HotelSearchRequest) []string {
	// Assuming the hotel codes are part of the 'rq' struct as a comma-separated string
	return strings.Split(rq.HotelCode, ",")
}

func splitHotelCodesIntoBatches(hotelCodes []string, batchSize int) [][]string {
	var batches [][]string
	for i := 0; i < len(hotelCodes); i += batchSize {
		end := i + batchSize
		if end > len(hotelCodes) {
			end = len(hotelCodes)
		}
		batches = append(batches, hotelCodes[i:end])
	}
	return batches
}

func shutdownProducer() {
	if producer != nil {
		_ = producer.Close()
	}
}

func waitForResponse(correlationID string) (map[string]interface{}, error) {
	listenersLock.Lock()
	listener, exists := listeners[correlationID]
	listenersLock.Unlock()

	if !exists {
		return nil, fmt.Errorf("no listener found for correlationId: %s", correlationID)
	}

	for {
		listenersLock.Lock()
		if listener.counter == listener.batchCount {
			// All batches processed, merge only the hotel_listing
			mergedResponse := make(map[string]interface{})
			var mergedHotelListings []interface{} // For merging hotel listings

			for _, resp := range listener.responses {
				if hotelListing, ok := resp["hotel_listing"]; ok {
					mergedHotelListings = append(mergedHotelListings, hotelListing)
				}
			}
			// Merge the hotel listings into the mergedResponse map
			mergedResponse["hotel_listing"] = mergedHotelListings

			listenersLock.Unlock()
			return mergedResponse, nil
		}
		listenersLock.Unlock()

		// Wait for response for a short period
		time.Sleep(100 * time.Millisecond)
	}
}

func StartConsumer() {
	// Ensure consumer starts once at the start of the app
	brokers := []string{os.Getenv("KAFKA_BROKER_URL")}
	log.Println("Starting Kafka consumer...")

	currentTime := time.Now().UnixNano() / int64(time.Millisecond)
	consumerGrp := "search-hotel-consumer" + strconv.FormatInt(currentTime, 10)
	config := sarama.NewConfig()
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Net.SASL.Enable = true
	config.Net.SASL.User = os.Getenv("KAFKA_USERNAME")
	config.Net.SASL.Password = os.Getenv("KAFKA_PASSWORD")
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512

	// Create Kafka consumer group
	consumerGroup, err := sarama.NewConsumerGroup(brokers, consumerGrp, config)
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
		messageData := string(message.Value)
		var responseData map[string]interface{}
		err := json.Unmarshal([]byte(messageData), &responseData)
		if err != nil {
			log.Printf("Failed to parse message: %v", err)
			continue
		}

		// Extract hotel_listing from the response data
		hotelListing := responseData["hotel_listing"]

		correlationID := string(message.Key)

		// Check if a listener exists for the given correlationId
		listenersLock.Lock()
		listener, exists := listeners[correlationID]
		if exists {
			// Append the hotel_listing to the listener's responses
			// Only store the hotel_listing in the response
			listener.responses = append(listener.responses, map[string]interface{}{
				"hotel_listing": hotelListing,
			})
			listener.counter++

			// fmt.Println("Listener counter: ", listener.responses)

			// If all batches are processed, send the response to the channel
			if listener.counter == listener.batchCount {
				select {
				case listener.responseCh <- listener.responses:
				default:
					log.Printf("Listener channel for correlationId %s is not ready", correlationID)
				}
			}
		}
		listenersLock.Unlock()

		// Mark the message as processed
		session.MarkMessage(message, "")
	}
	return nil
}
