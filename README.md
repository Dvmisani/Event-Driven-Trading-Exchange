# Event\-Driven Trading Exchange

A production\-style trading exchange inspired by modern financial market infrastructure such as NASDAQ, JSE, and Binance.

Built with C\#, .NET 10, Apache Kafka, SQL Server, SignalR, CQRS, and Event Sourcing, the platform processes orders through an event\-driven architecture, maintains immutable audit history, streams real\-time market data, and demonstrates horizontal scalability using partitioned message processing.

## Live Demonstration

- Video Walkthrough: Coming Soon
- Architecture Overview: Coming Soon
- Azure Deployment: Coming Soon

---

## Why This Project?

Most portfolio projects demonstrate CRUD operations.

Real exchanges face a different set of engineering challenges:

- High\-throughput order processing
- Deterministic trade execution
- Immutable audit trails
- Real\-time market data distribution
- Event replay and recovery
- Horizontal scalability
- Fault tolerance under load

This project was built to demonstrate how those challenges can be addressed using patterns commonly found in production\-grade distributed systems.

---

## Core Features

### Event\-Sourced Matching Engine

- Price\-Time Priority (FIFO) order matching
- Immutable event streams
- Full event replay capability
- Deterministic trade execution

### CQRS Architecture

- Commands separated from query workloads
- Write model optimized for consistency
- Read model optimized for performance
- Independent scalability of read and write operations

### Apache Kafka Event Backbone

- Symbol\-based partitioning
- Ordered event processing
- Dead Letter Queue (DLQ) support
- Idempotent event consumption

### Real\-Time Market Data

- SignalR WebSocket streaming
- Live trade broadcasting
- Real\-time order book updates
- Immediate client synchronization

### Observability

- OpenTelemetry distributed tracing
- Jaeger trace visualization
- Prometheus metrics collection
- Grafana dashboards

### Production Engineering Practices

- Docker containerization
- Infrastructure as Code (Bicep)
- Automated CI/CD pipelines
- Integration testing
- Performance benchmarking

---

## System Architecture

                    React Trading Terminal

                               │

                               ▼

                     ASP.NET Core API

                               │

             ┌─────────────────┴─────────────────┐

             ▼                                   ▼

      Command Handlers                   Query Handlers

             │                                   │

             ▼                                   ▼

      Matching Engine                     Read Models

             │                                   ▲

             ▼                                   │

      SQL Event Store                            │

             │                                   │

             ▼                                   │

         Apache Kafka ─────────► Projection Worker

             │                                   │

             └──────────────► SignalR Hub ──────┘

                                         │

                                         ▼

                                 Connected Clients

---

## Technology Stack

| Layer | Technology |
| --- | --- |
| Backend | .NET 10, ASP.NET Core |
| Messaging | Apache Kafka |
| Database | SQL Server |
| Real\-Time Communication | SignalR |
| Architecture | Event Sourcing, CQRS |
| Testing | xUnit, Testcontainers, NBomber |
| Observability | OpenTelemetry, Jaeger, Prometheus, Grafana |
| Cloud | Azure Container Apps, Azure SQL, Azure Event Hubs |
| Infrastructure | Docker, Bicep, GitHub Actions |

---

## Performance Benchmarks

### Order Processing

- Sustained throughput: 113,123 orders per second
- Event replay throughput: 1.9 million events per second

### Reliability

- Idempotent command handling
- Event persistence guarantees
- Dead Letter Queue recovery workflow
- Deterministic replay validation

Benchmarking performed using NBomber under containerized test environments.

---

## Engineering Challenges Solved

### Event Ordering

Guarantees FIFO matching behaviour per instrument using Kafka partition keys aligned to trading symbols.

### State Recovery

Allows complete exchange recovery by rebuilding projections from stored event streams.

### Scalability

Read and write workloads scale independently through CQRS and partitioned event processing.

### Traceability

Every trade, order, cancellation, and projection update can be traced end\-to\-end through OpenTelemetry instrumentation.

---

## Testing Strategy

### Unit Tests

Validation of matching engine logic and domain rules.

### Integration Tests

End\-to\-end verification across:

- API
- SQL Server
- Kafka
- SignalR
- Projection services

### Performance Tests

NBomber workloads validate:

- Throughput
- Latency
- Replay performance
- Resource utilization

---

## Key Engineering Lessons

Building this platform deepened expertise in:

- Event Sourcing
- CQRS
- Apache Kafka
- Distributed Systems Design
- Real\-Time Streaming Architectures
- High\-Performance .NET Development
- Observability and Monitoring
- Financial Market Infrastructure

---

## Future Enhancements

- Multi\-asset trading support
- Risk and margin management
- Market surveillance engine
- FIX protocol integration
- Kubernetes deployment
- Multi\-region replication
- Advanced order types
- AI\-powered anomaly detection

---

## Author

Dumisani A. Baloyi

Software Engineer focused on Backend Development, Cloud Engineering, Enterprise AI, and Financial Systems.

