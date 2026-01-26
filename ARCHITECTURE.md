# Architecture Deep-Dive

This document provides a technical deep-dive into the E-commerce Streaming Pipeline architecture.

![Architecture Diagram](/diagrams/architecture.png)

## Component Breakdown

### 1. Data Generation (Python Script)
- **Role**: Simulates a traffic e-commerce platform.
- **Logic**: Simulates purchases based on `TV_DATASET_USA.csv` catalog, randomizing customers and transaction details.
- **Protocol**: Produces messages to Kafka topics using the `confluent_kafka` library.

### 2. Stream Processing (Apache Kafka on Confluent Cloud)
- **Role**:  event streaming platform.
- **Topic**: `ecommerce-topic-1`.
- **Security**: SASL_SSL authentication.

### 3. Real-Time Analytics (Apache Druid)
- **Role**: High-performance, real-time analytics database.
- **Deployment**: AWS EC2 (m7i-flex.large).
- **Storage**

### 4. Visualization (Grafana)
- **Role**: Operational dashboards.
- **Connection**: Uses the Druid datasource plugin.
- **Panels**
  - Real-time line charts for traffic.
  - Bar charts for top products.
  - Stat panels for KPI metrics 

## Data Flow
1. `kafka_producer.py` creates JSON events -> Kafka.
2. Druid Supervisor task monitors Kafka topic.
3. Druid MiddleManager tasks ingest and index data in real-time.

## Scalability Considerations
- **Kafka**: Managed service scales automatically/per partition.
- **Druid**: Can scale horizontally by adding more MiddleManagers/Historicals.