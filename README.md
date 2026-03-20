# MIDAS Core

MIDAS Core is a Spring Boot transaction-processing service completed as part of the JPMorgan Chase & Co. Advanced Software Engineering virtual experience on Forage.

## What This Project Demonstrates

- Spring Boot application setup with Java 17
- REST endpoint design with a balance lookup API
- Kafka consumer flow for transaction events
- persistence with Spring Data JPA
- transaction validation and balance updates
- integration with the provided incentive service artifact

## Tech Stack

- Java 17
- Spring Boot
- Spring Web
- Spring Data JPA
- Spring Kafka
- H2 Database
- Maven
- JUnit

## Core Functionality

- `GET /balance?userId=<id>` returns the current balance for a user
- transaction messages are consumed from the configured Kafka topic
- sender and recipient are validated before processing
- balances are updated only when the sender has sufficient funds
- incentive values are fetched from the provided `transaction-incentive-api.jar`
- processed transactions are stored through JPA repositories

## Project Structure

```text
src/main/java/com/jpmc/midascore/
├── MidasCoreApplication.java
├── component/
│   ├── BalanceController.java
│   └── DatabaseConduit.java
├── entity/
├── foundation/
└── repository/
```

## Running Locally

```bash
./mvnw test
```

The project uses the configuration in [`application.yml`](./application.yml) and the provided incentive service JAR in [`services/transaction-incentive-api.jar`](./services/transaction-incentive-api.jar).

## Notes

This repository reflects the completed exercise solution and keeps the provided test harness and service artifact that were part of the program environment.
