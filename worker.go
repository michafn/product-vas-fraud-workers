package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/streadway/amqp"
)

const (
	pagingLimit    int           = 200
	requestTimeout time.Duration = time.Second * 30
)

type FraudCases struct {
	CdlId                      string `json:"cdlId"`
	DateOfAttack               int64  `json:"dateOfAttack"`
	Type                       string `json:"type"`
	BusinessPartnerCountryCode string `json:"businessPartnerCountryCode"`

	BankAccount struct {
		BankCountryCode string `json:"bankCountryCode"`
	}
}

type FraudCasesResponse struct {
	Page          int          `json:"page"`
	NumberOfPages int          `json:"numberOfPages"`
	FraudCases    []FraudCases `json:"fraudCases"`
}

func main() {
	err := validateEnvVars()
	failOnError("Failed to validate required env vars", err)

	initSentry()

	url := os.Getenv("RMQ_AMQP_URL")
	queueName := os.Getenv("RMQ_QUEUE_NAME")
	subscribe(url, queueName)
}

func validateEnvVars() error {
	for _, envVarName := range [...]string{
		"RMQ_AMQP_URL",
		"RMQ_QUEUE_NAME",
		"SENTRY_DSN",
		"CDQ_FRAUD_CASES_API_URL",
		"CATENAX_API_URL",
		"CATENAX_API_KEY",
	} {
		if _, ok := os.LookupEnv(envVarName); !ok {
			return fmt.Errorf("Env var $%s is missing", envVarName)
		}
	}

	return nil
}

func isDebugModeEnabled() bool {
	if debugEnvVar, ok := os.LookupEnv("DEBUG"); ok {
		parsedValue, err := strconv.ParseBool(debugEnvVar)
		if err != nil {
			fmt.Printf("Unable to parse env var $DEBUG=%s as bool\n", debugEnvVar)
			return false
		}

		return parsedValue
	}

	return false
}

func initSentry() {
	err := sentry.Init(sentry.ClientOptions{
		Dsn:   os.Getenv("SENTRY_DSN"),
		Debug: isDebugModeEnabled(),
	})
	failOnError("Failed to initialize Sentry", err)
}

func subscribe(url string, queueName string) {
	conn, err := amqp.Dial(url)
	failOnError("Failed to connect to RabbitMQ", err)
	defer conn.Close()

	channel, err := conn.Channel()
	failOnError("Failed to open a channel", err)
	defer channel.Close()

	msgs, err := channel.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	failOnError("Failed to register a consumer", err)

	for msg := range msgs {
		err := handleMessage(&msg)
		if err != nil {
			log.Printf("Failed to handle message with error: %v", err)
			log.Printf("Dropping message: %s", string(msg.Body))
			msg.Nack(false, false)
		} else {
			log.Printf("Successfully processed message: %d", msg.DeliveryTag)
			msg.Ack(false)
		}
	}
}

func handleMessage(msg *amqp.Delivery) error {
	fraudCasesApiKey := string(msg.Body)

	page := 0
	fraudCasesResponse := fetchFraudCases(fraudCasesApiKey, page)
	oldestUpdatedAt := upsertFraudCases(fraudCasesResponse.FraudCases)

	for page = 1; page < fraudCasesResponse.NumberOfPages; page++ {
		fraudCasesResponse := fetchFraudCases(fraudCasesApiKey, page)
		updatedAt := upsertFraudCases(fraudCasesResponse.FraudCases)
		if updatedAt.Before(oldestUpdatedAt) {
			oldestUpdatedAt = updatedAt
		}
	}

	deleteFraudCases(oldestUpdatedAt)
	return nil
}

func fetchFraudCases(fraudCasesApiKey string, page int) *FraudCasesResponse {
	req := createGetFraudCasesRequest(fraudCasesApiKey, page)

	client := &http.Client{Timeout: requestTimeout}
	resp, err := client.Do(req)
	failOnError("Failed to get fraud cases", err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	failOnError("Failed to read response body", err)

	var fraudCasesResponse FraudCasesResponse
	err = json.Unmarshal(body, &fraudCasesResponse)
	failOnError("Failed to parse JSON", err)
	return &fraudCasesResponse
}

func createGetFraudCasesRequest(fraudCasesApiKey string, page int) *http.Request {
	url := os.Getenv("CDQ_FRAUD_CASES_API_URL")
	req, err := http.NewRequest(http.MethodGet, url, nil)
	failOnError("Failed to create GET request", err)

	// Set headers
	req.Header.Set("X-API-KEY", fraudCasesApiKey)

	// Add query params
	q := req.URL.Query()
	q.Add("classification", "CATENAX")
	q.Add("pageSize", strconv.Itoa(pagingLimit))
	q.Add("page", strconv.Itoa(page))

	req.URL.RawQuery = q.Encode()
	return req
}

func upsertFraudCases(fraudCases []FraudCases) time.Time {
	req := createUpsertFraudCasesRequest(fraudCases)
	client := &http.Client{Timeout: requestTimeout}
	resp, err := client.Do(req)
	failOnError("Failed to upsert fraud cases", err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	failOnError("Failed to read response body", err)
	log.Printf("Response: %s\n", string(body))

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Upsert request failed with status code %d", resp.StatusCode)
	}

	type UpsertFraudCasesResponse struct {
		UpdatedAt time.Time `json:"updatedAt"`
	}

	var parsedResponse UpsertFraudCasesResponse
	err = json.Unmarshal(body, &parsedResponse)
	failOnError("Failed to parse JSON", err)
	return parsedResponse.UpdatedAt
}

func createUpsertFraudCasesRequest(fraudCases []FraudCases) *http.Request {
	type FraudCaseObj struct {
		CdlId        string `json:"cdlId"`
		DateOfAttack int64  `json:"dateOfAttack"`
		Type         string `json:"type"`
		CountryCode  string `json:"countryCode"`
	}

	fraudCasesObjects := make([]FraudCaseObj, 0, len(fraudCases))
	for _, fc := range fraudCases {

		countryCode := fc.BankAccount.BankCountryCode
		if len(countryCode) == 0 {
			countryCode = fc.BusinessPartnerCountryCode

		/*	if len(countryCode) == 0 {
				log.Printf("Skipping fraud case with id %s due to missing country code", fc.CdlId)
				continue
			}
		*/
		}

		fraudCasesObjects = append(fraudCasesObjects, FraudCaseObj{
			CdlId:        fc.CdlId,
			DateOfAttack: fc.DateOfAttack,
			Type:         fc.Type,
			CountryCode:  countryCode,
		})
	}

	bodyBytes, err := json.Marshal(fraudCasesObjects)
	failOnError("Failed to marshal request body", err)

	url := os.Getenv("CATENAX_API_URL")
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(bodyBytes))
	failOnError("Failed to create PUT request", err)

	// Set headers
	req.Header.Set("X-API-KEY", os.Getenv("CATENAX_API_KEY"))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func deleteFraudCases(updatedAt time.Time) {
	req := createDeleteFraudCasesRequest(updatedAt)
	client := &http.Client{Timeout: requestTimeout}
	resp, err := client.Do(req)
	failOnError("Failed to delete fraud cases", err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	failOnError("Failed to read response body", err)
	if len(body) > 0 {
		log.Printf("Response: %s\n", string(body))
	}

	if resp.StatusCode != http.StatusNoContent {
		log.Fatalf("Delete request failed with status code %d", resp.StatusCode)
	}
}

func createDeleteFraudCasesRequest(updatedAt time.Time) *http.Request {
	url := os.Getenv("CATENAX_API_URL")
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	failOnError("Failed to create DELETE request", err)

	// Set headers
	req.Header.Set("X-API-KEY", os.Getenv("CATENAX_API_KEY"))

	// Add query params
	q := req.URL.Query()
	q.Add("latest", updatedAt.Format(time.RFC3339))
	req.URL.RawQuery = q.Encode()
	return req
}

func failOnError(msg string, err error) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
