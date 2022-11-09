package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"product-tax-microservice/internal/order/infra/database"
	"product-tax-microservice/internal/order/usecase"
	"product-tax-microservice/pkg/rabbitmq"
	"time"

	_ "github.com/mattn/go-sqlite3"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	showAppLogo()

	db, err := sql.Open("sqlite3", "./orders.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	repository := database.NewOrderReposity(db)
	uc := usecase.CalculateFinalPriceUseCase{OrderRepository: repository}

	ch, err := rabbitmq.OpenChannel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()
	out := make(chan amqp.Delivery) // cria o Channel
	go rabbitmq.Consume(ch, out)    // Thread 2

	for msg := range out {
		var inputDTO usecase.OrderInputDTO

		err := json.Unmarshal(msg.Body, &inputDTO)
		if err != nil {
			panic(err)
		}
		outputDTO, err := uc.Execute(inputDTO)
		if err != nil {
			panic(err)
		}
		msg.Ack(false)
		fmt.Println(outputDTO)
		time.Sleep(50 * time.Millisecond)
	}
}

func showAppLogo() {
	println("--------->> LEGATES <<--------- \n---->> APLICAÇÃO RODANDO <<----")
}
