# Spring Boot Kafka Demo with Redpanda

This project demonstrates a simple Kafka-based application using Spring Boot, Redpanda, and various other technologies. It is designed to showcase how Kafka can be integrated with Spring Boot for message-driven microservices and includes basic CRUD operations with JPA.

## Technologies Used

- **Spring Boot**: Framework for building Java-based applications.
- **Spring Web**: To expose REST APIs.
- **Spring Data JPA**: For ORM and database interactions.
- **Apache Kafka**: Distributed event streaming platform.
- **Redpanda**: Kafka-compatible streaming platform used as a Kafka alternative.
- **Lombok**: To reduce boilerplate code for Java entities and classes.
- **Maven**: Project management and build tool.
- **Java**: Programming language used for application logic.

## Features

- REST API for CRUD operations on an entity (e.g., User).
- Kafka producer to send messages to a Kafka topic.
- Kafka consumer to consume messages from the topic.
- Redpanda used as a Kafka broker for high-performance message streaming.

## Prerequisites

Make sure you have the following installed:

- Java 17
- Maven
- Docker (for running Redpanda)
