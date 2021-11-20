package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/ozonmp/week-6-workshop/kafka"
	"github.com/ozonmp/week-6-workshop/protos"
	"google.golang.org/protobuf/proto"
)

type DB struct {
	sync.Mutex
	Orders map[int64]protos.Order
	OrdersStatuses map[int64]int
}

var db DB
var producer sarama.SyncProducer

const (
	OrderCreated = iota
	OrderCommited
)

func CreateOrder(ctx context.Context, message *sarama.ConsumerMessage) error {
	var order protos.Order

	err := proto.Unmarshal(message.Value, &order)
	if err != nil {
		return err
	}

	db.Lock()
	defer db.Unlock()

	if _, ok := db.Orders[order.OrderID]; ok {
		fmt.Printf("Order with ID %d already exists", order.OrderID)
		return nil
	}

	db.Orders[order.OrderID] = order
	db.OrdersStatuses[order.OrderID] = OrderCreated

	if rand.Intn(10) == 1 {
		fmt.Printf("Make order ID %d as failed", order.OrderID)
		err = kafka.SendMessage(producer, "cancel_order", message.Value)
		if err != nil {
			return err
		}
	}


	return nil
}

func CommitOrder(ctx context.Context, message *sarama.ConsumerMessage) error {
	var order protos.Order

	err := proto.Unmarshal(message.Value, &order)
	if err != nil {
		return err
	}

	db.Lock()
	defer db.Unlock()

	if _, ok := db.Orders[order.OrderID]; ok {
		fmt.Printf("Order with ID %d commited", order.OrderID)
		db.OrdersStatuses[order.OrderID] = OrderCommited
		return nil
	}

	return nil
}

func CancelOrder(ctx context.Context, message *sarama.ConsumerMessage) error {
	var order protos.Order

	err := proto.Unmarshal(message.Value, &order)
	if err != nil {
		return err
	}

	db.Lock()
	defer db.Unlock()

	if _, ok := db.Orders[order.OrderID]; ok {
		fmt.Printf("Order with ID %d canceled", order.OrderID)
		delete(db.Orders, order.OrderID)
		return nil
	}

	return nil
}

func main() {
	brokers := []string{"127.0.0.1:9095", "127.0.0.1:9096", "127.0.0.1:9097"}

	ctx := context.Background()

	var err error
	producer, err = kafka.NewSyncProducer(brokers)
	if err != nil {
		log.Fatal(err)
	}

	err = kafka.StartConsuming(ctx, brokers, "create_order", CreateOrder)
	if err != nil {
		log.Fatal(err)
	}
	err = kafka.StartConsuming(ctx, brokers, "commit_order", CommitOrder)
	if err != nil {
		log.Fatal(err)
	}
	err = kafka.StartConsuming(ctx, brokers, "cancel_order", CancelOrder)
	if err != nil {
		log.Fatal(err)
	}

	for {

	}
}

