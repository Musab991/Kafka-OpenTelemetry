# 🚛 Real-Time AVL Telemetry System with Apache Kafka

This project is a distributed, event-driven Automatic Vehicle Location (AVL) system. It simulates real-time GPS and telemetry data from a fleet of vehicles and processes it using a high-availability Apache Kafka cluster.

## 🏗️ Architecture overview

The system is built using a microservices approach, fully containerized with Docker Compose:

* **Kafka Infrastructure:** A 3-node Apache Kafka cluster running in **KRaft mode** (ZooKeeper-less) to handle high-throughput telemetry streams with High Availability (Replication Factor: 3).
* **AVL Sensor Producer (C# / .NET 8):** Simulates vehicle movement, generating structured JSON payloads containing Latitude, Longitude, Speed, and IMEI, and publishes them to the `avl-signals` topic.
* **Real-Time Tracker Consumer (C# / .NET 8):** Subscribes to the live telemetry stream to process vehicle locations.
* **Overspeed Alert Monitor (C# / .NET 8):** An independent consumer group that monitors the stream specifically for speed limit violations.
* **Kafka UI:** A web-based dashboard to monitor cluster health, topics, and consumer lag.

## 🛠️ Tech Stack

* **Language/Framework:** C#, .NET 8
* **Message Broker:** Apache Kafka 7.8 (KRaft Mode)
* **Libraries:** `Confluent.Kafka`, `Serilog`
* **Infrastructure:** Docker, Docker Compose
* **Monitoring:** Provectus Kafka UI

## 🚀 Getting Started

### Prerequisites
* Docker and Docker Desktop installed.
* Ensure Docker has file-sharing permissions for the project drive.

### Running the Cluster

1. Clone the repository and navigate to the infrastructure folder:
   ```bash
   git clone [your-repo-url]
   cd [your-folder-name]
