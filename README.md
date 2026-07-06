# Mini-Twitter Real-Time — Progress Log

A simplified real-time Twitter-like pipeline built with Apache Kafka: a producer generates
random tweets, which are then consumed, filtered (stateless), and aggregated (stateful)
using Kafka Streams — with an optional MongoDB storage layer.

## Environment setup

- **Java**: JDK 25 (Oracle), installed and verified with `java --version`
- **Maven**: version 3.8.7, installed and verified with `mvn --version`
- **Docker**: version 29.6.1, installed to run Kafka + Zookeeper locally
- **Git**: local repository initialized (`git init`), branch renamed to `main`, identity configured (`user.name` / `user.email`)

## Project structure
mini-twitter-kafka/
├── .gitignore
├── pom.xml
└── src/main/java/com/manaess/kafka/tweets/
├── TweetProducer.java
└── TweetConsumer.java

## What has been done

### 1. Maven configuration (`pom.xml`)

Project configuration file defining:
- Java 17 as the compilation target
- Dependencies: `kafka-clients`, `kafka-streams`, `gson` (JSON), `mongodb-driver-sync` (bonus)
- Plugins: `exec-maven-plugin` (to easily run classes), `maven-shade-plugin` (to package a jar bundled with all dependencies)

Compilation successfully tested (`mvn clean compile` → `BUILD SUCCESS`).

### 2. `TweetProducer.java`

Generates 100 random tweets (`user_id`, `timestamp`, `tweet_text`, `hashtags`) in the required format, wrapped in a JSON object `{"message": "..."}`, and sends them to the Kafka topic `tweets-input` via a `KafkaProducer`.

### 3. `TweetConsumer.java`

Consumes messages from the `tweets-input` topic, extracts the `"message"` field from the JSON, then parses each sub-field with a simple `split()` (`user_id`, `timestamp`, `tweet_text`, `hashtags`) and prints them to the console.
