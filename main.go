package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/labstack/echo"
	"github.com/labstack/gommon/random"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type Message struct {
	Timestamp string `json:"timestamp"`
	Sender    string `json:"sender"`
	Receiver  string `json:"receiver"`
	Message   string `json:"message"`
}

func main() {
	e := echo.New()
	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "Hello, World!")
	})

	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	e.POST("/messages/:id", postMessage)
	e.GET("/messages/:id", getMessages)

	server := &http.Server{
		Addr: ":8080",
	}

	server.Handler = h2c.NewHandler(e, &http2.Server{})
	e.Logger.Fatal(e.StartServer(server))
}

func postMessage(c echo.Context) error {
	id := c.Param("id")
	config := sarama.NewConfig()
	config.ClientID = fmt.Sprint("messages-", id)
	config.Producer.Retry.Max = 1
	config.Producer.Return.Successes = true
	config.Producer.Timeout = time.Second

	producer, err := sarama.NewAsyncProducer([]string{"35.237.156.150:9092"}, config)
	if err != nil {
		return c.String(http.StatusInternalServerError, "unable to connect to kafka\n")
	}
	defer producer.Close()

	bytes, _ := ioutil.ReadAll(c.Request().Body)
	msg := Message{}
	if err := json.Unmarshal(bytes, &msg); err != nil {
		return c.String(http.StatusBadRequest, "invalid message payload")
	}

	producer.Input() <- &sarama.ProducerMessage{
		Value: sarama.ByteEncoder(bytes),
		Timestamp: time.Now(),
		Key: sarama.StringEncoder(fmt.Sprintf("messages-%s-%d", id, time.Now().Unix())),
		Topic: fmt.Sprintf("messages-%s", id),
	}

	return c.String(200, "posted successfully")
}

func getMessages(c echo.Context) error {
	id := c.Param("id")
	start := c.QueryParam("start")
	offset := sarama.OffsetOldest
	if start == "false" {
		offset = sarama.OffsetNewest
	}
	config := sarama.NewConfig()
	config.ClientID = fmt.Sprint("messages-", id, "-", random.New().String(2, random.Alphanumeric))
	config.Consumer.MaxWaitTime = time.Second
	config.Consumer.Retry.Backoff = time.Second

	consumer, err := sarama.NewConsumer([]string{"35.237.156.150:9092"}, config)
	if err != nil {
		return c.String(http.StatusInternalServerError, "unable to connect to kafka\n")
	}

	cp, err := consumer.ConsumePartition(fmt.Sprintf("messages-%s", id), 0, offset)
	if err != nil {
		return c.String(http.StatusInternalServerError, "unable to connect to kafka consumer partition\n")
	}

	notify := c.Request().Context()

	go func() {
		<- notify.Done()
		cp.Close()
		consumer.Close()
	}()

	writer := c.Response().Writer
	c.Response().Header().Set("Cache-Control", "no-cache")
	c.Response().Header().Set("Connection", "keep-alive")
	flusher := writer.(http.Flusher)

	flusher.Flush()
	for msg := range cp.Messages() {
		m := Message{}
		if err := json.Unmarshal(msg.Value, &m); err == nil {
			fmt.Fprintf(writer, "%s\n", string(msg.Value))
			flusher.Flush()
		}

	}

	return c.String(http.StatusOK, id)
}