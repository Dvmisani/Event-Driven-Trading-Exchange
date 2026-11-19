# Event-Driven Trading Exchange (.NET 10 • Kafka • SQL Server 2025 • SignalR)

[![Build & Test (SQL Server 2025 + Kafka)](https://github.com/Dvmisani/Event-Driven-Trading-Exchange/actions/workflows/ci.yml/badge.svg)](https://github.com/Dvmisani/Event-Driven-Trading-Exchange/actions/workflows/ci.yml)

Simplified institutional backend trading exchange modeled on **JSE**, **NASDAQ**, and **Binance** market mechanics—built with **C# / .NET 10**, **Event Sourcing**, **CQRS**, **Apache Kafka**, **SQL Server 2025**, and **SignalR WebSockets**.

## Core Engineering Patterns

- **Price-Time Priority Matching Engine:** Matches aggressive orders against resting limit books (`Bids` descending, `Asks` ascending, FIFO within price levels) at the resting maker's price.
- **Pure Event Sourcing:** `EvaluatePlaceOrder` and `EvaluateCancelOrder` never mutate state; every action emits immutable domain events (`OrderPlaced`, `OrderMatched`, `TradeExecuted`, `OrderCancelled`) persisted to an append-only SQL Server Event Store and replayable on startup or via `/api/admin/rebuild/{symbol}`.
- **Strict CQRS + Apache Kafka:** Commands write to the Event Store and publish keyed by `Symbol` to Kafka (`exchange.events`), while an idempotent `BackgroundService` consumer projects read models (`Orders`, `Trades`, `DepthLevels`).
- **Real-Time SignalR Market Data:** Pushes live order book depth snapshots (`OrderBookUpdated`) and trade ticks (`TradeExecuted`) over WebSockets to subscribed clients (`/hubs/marketdata`).

## System Architecture

```mermaid
flowchart LR
    Client["Trading Client / UI"] -->|POST /api/orders| Api["Command API (.NET 10)"]
    Api --> Engine["EventSourcedOrderBook<br/>(Price-Time Priority)"]
    Engine -->|Append Events| Store[("SQL Server Event Store")]
    Engine -->|Publish Keyed by Symbol| Kafka[["Apache Kafka (exchange.events)"]]
    Kafka --> Worker["CQRS Projection Worker"]
    Worker --> ReadDb[("SQL Server Read Models<br/>Orders • Trades • Depth")]
    Worker -->|Real-Time Push| Hub["SignalR Hub (/hubs/marketdata)"]
    Hub -->|WebSockets| Feed["Live Depth & Trade Tape"]
```

## API Endpoints & Quickstart

| Method | Endpoint | Side | Description |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/orders` | Command | Submit limit `Buy` / `Sell` order; returns emitted domain events |
| `DELETE` | `/api/orders/{symbol}/{orderId}` | Command | Cancel active resting order and emit `OrderCancelled` |
| `POST` | `/api/admin/rebuild/{symbol}` | Admin | Replay SQL Server event stream from sequence `1` to rebuild state |
| `GET` | `/api/marketdata/{symbol}/orderbook` | Query | Aggregated L2 order book depth (`Bids` and `Asks`) |
| `GET` | `/api/marketdata/{symbol}/trades` | Query | Recent executed trades tape |
| `GET` | `/api/marketdata/{symbol}/events` | Query | Immutable ordered event store log |

```bash
# Run full stack (API on :5080, Kafka UI on :8085, SQL Server 2025 on :1433)
docker compose up --build

# Run unit replay + end-to-end Testcontainers suite
dotnet test --nologo --logger "console;verbosity=normal"
```

## Kafka Partitioning & Fault-Tolerance Architecture

All domain events are published to `exchange.events` using `Key = Symbol` (Murmur2 consistent hashing) with `Acks = All` and `EnableIdempotence = true`:

```text
AAPL   ──▶ Partition 1 ──▶ Strict FIFO per-symbol order lifecycle (OrderPlaced → OrderMatched → TradeExecuted)
MSFT   ──▶ Partition 2 ──▶ Parallel horizontal throughput without cross-symbol lock contention
BTCUSD ──▶ Partition 3 ──▶ Independent consumer scaling across partition groups
```

- **End-to-End Idempotency:** Command retries carrying `ClientRequestId` (or `X-Idempotency-Key`) return the cached response without duplicate matching, while `ProjectionApplier` deduplicates at-least-once Kafka deliveries by `EventId`.
- **Dead Letter Queue (`exchange.events.deadletter`):** Poison or malformed messages failing bounded projection retries are routed to `exchange.events.deadletter` (and logged in SQL Server `/api/admin/deadletters`) before committing the partition offset so one bad payload never stalls an entire symbol stream.
