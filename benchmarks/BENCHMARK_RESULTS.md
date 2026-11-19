## Performance Benchmarks (NBomber)

Measured locally using **NBomber** on **Intel(R) Core(TM) i5-10310U CPU @ 1.70GHz** (15.8 GB RAM, `.NET 10.0.15`):

| Metric | Target | Measured Result | Latency (Mean / p95) | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Order Submissions & Matching** | `10,000+ orders/sec` | **110,455 orders/sec** | `0.26 ms` / `0.01 ms` | PASS |
| **Matching Engine Latency** | `< 5.00 ms` | **0.26 ms** (p99: `6.11 ms`) | `0.00 ms` p50 | PASS |
| **Event Stream Replay Speed** | `100,000+ events/sec` | **554,966 events/sec** (250,000 events in `450.5 ms`) | `1.80 µs/event` | PASS |
| **SignalR Market Data Broadcasts** | `1,000+ broadcasts/sec` | **336,198 broadcasts/sec** | `0.04 ms` / `0.03 ms` | PASS |