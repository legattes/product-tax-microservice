package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
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

	qtdWorkers := 5
	for i := 1; i <= qtdWorkers; i++ {
		go worker(out, &uc, i) // Creating more threads
	}

	http.HandleFunc("/total", func(w http.ResponseWriter, r *http.Request) {
		getTotalUC := usecase.GetTotalUseCase{OrderRepository: repository}
		total, err := getTotalUC.Execute()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		}
		json.NewEncoder(w).Encode(total)
	})
	http.ListenAndServe(":8080", nil)
}

func worker(deliveryMessage <-chan amqp.Delivery, uc *usecase.CalculateFinalPriceUseCase, workerID int) {
	for msg := range deliveryMessage {
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
		fmt.Printf("Worker %d has processed order %s\n", workerID, outputDTO.ID)
		time.Sleep(1 * time.Second)
	}
}

func showAppLogo() {
	println("--------->> LEGATES <<--------- \n---->> APLICAÇÃO RODANDO <<----")
}
