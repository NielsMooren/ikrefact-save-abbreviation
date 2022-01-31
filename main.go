package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
	"log"
	"os"
)

type AbbreviationRequest struct {
	Status        string                    `json:"status"`
	Abbreviation  string                    `json:"abbreviation"`
	Definition    string                    `json:"definition"`
	Url           string                    `json:"url"`
	Organisations []Organisation			`json:"organisations"`
}

type Organisation struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

func init() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalln("Error loading .env file")
	}
}

func main() {
	Consume()
}

func Consume() {
	var rabbitHost = os.Getenv("RABBIT_HOST")
	var rabbitPort = os.Getenv("RABBIT_PORT")
	var rabbitUser = os.Getenv("RABBIT_USERNAME")
	var rabbitPassword = os.Getenv("RABBIT_PASSWORD")
	var queueName = os.Getenv("RABBIT_QUEUE_NAME")
	var consumerTag = os.Getenv("RABBIT_CONSUMER_TAG")

	var host = os.Getenv("DATABASE_HOST")
	var port = os.Getenv("DATABASE_PORT")
	var user = os.Getenv("DATABASE_USER")
	var password = os.Getenv("DATABASE_PASSWORD")
	var dbname = os.Getenv("DATABASE_DBNAME")

	conn, err := amqp.Dial("amqp://" + rabbitUser + ":" + rabbitPassword + "@" + rabbitHost + ":" + rabbitPort + "/")
	failOnError(err, "Failed to connect to RabbitMQ")

	defer conn.Close()

	channel, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer channel.Close()

	queue, err := channel.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil)
	failOnError(err, "Failed to declare a queue")

	fmt.Println("Channel and Queue established")

	deliveries, err := channel.Consume(queue.Name,
		consumerTag,
		false,
		false,
		false,
		false,
		nil)
	failOnError(err, "Failed to register consumer")

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)

	if err != nil {
		panic(err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {

		}
	}(db)

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Connected to database: %s\n", dbname)

	insertIntoAbbreviation := `INSERT INTO abbreviation (id, abbreviation, definition, status, url) values (gen_random_uuid(), $1, $2, $3, $4) RETURNING id`
	insertIntoAbbreviationOrganisation := `INSERT INTO abbreviation_organisation (abbreviation_id, organisation_id) values ($1, $2)`
	id := ""

	forever := make(chan bool)
	go func() {
		for message := range deliveries {
			var abbreviationRequest AbbreviationRequest

			err := json.Unmarshal(message.Body, &abbreviationRequest)
			if err != nil {
				fmt.Println(err)
				return
			}

			err = db.QueryRow(insertIntoAbbreviation, abbreviationRequest.Abbreviation, abbreviationRequest.Definition, abbreviationRequest.Status, abbreviationRequest.Url).Scan(&id)
			if err != nil {
				log.Println(err)
				log.Fatalln("Error inserting record into Abbreviation!")
				return
			}

			db.QueryRow(insertIntoAbbreviationOrganisation, id, abbreviationRequest.Organisations[0].Id)

			message.Ack(false)
		}
	}()

	fmt.Println("Running...")
	<-forever
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
