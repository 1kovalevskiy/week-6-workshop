package main

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/ozonmp/week-6-workshop/kafka"
	"github.com/ozonmp/week-6-workshop/protos"
	"google.golang.org/protobuf/proto"
)

const (
	reserveProducts = "reserve_products"
)

func handleReserveOrders(ctx context.Context, message *sarama.ConsumerMessage) error {
	var order protos.Order

	err := proto.Unmarshal(message.Value, &order)
	if err != nil {
		fmt.Printf("unmarshall error: %")
	}

	return nil
}

func main() {
	brokers := []string{"127.0.0.1:9095", "127.0.0.1:9096", "127.0.0.1:9097"}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	kafka.StartConsuming(ctx, brokers, reserveProducts, handleReserveOrders)
}
