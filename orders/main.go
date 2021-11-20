package main

import (
	"context"
	"fmt"
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

const (
	OrderCreated = iota
	OrderCommited
	OrderCancelled
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

	return nil
}

func main() {
	brokers := []string{"127.0.0.1:9095", "127.0.0.1:9096", "127.0.0.1:9097"}

	ctx := context.Background()

	kafka.StartConsuming(ctx, brokers, "create_order")
	kafka.StartConsuming(ctx, brokers, "commit_order")
	kafka.StartConsuming(ctx, brokers, "cacnel_order")
}

