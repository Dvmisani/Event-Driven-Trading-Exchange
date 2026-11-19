## Performance Benchmarks (NBomber)

Measured locally using **NBomber** on **Intel(R) Core(TM) i5-10310U CPU @ 1.70GHz** (15.8 GB RAM, `.NET 10.0.15`):

| Metric | Target | Measured Result | Latency (Mean / p95) | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Order Submissions & Matching** | `10,000+ orders/sec` | **113,123 orders/sec** | `0.14 ms` / `0.01 ms` | PASS |
| **Matching Engine Latency** | `< 5.00 ms` | **0.14 ms** (p99: `0.38 ms`) | `0.00 ms` p50 | PASS |
| **Event Stream Replay Speed** | `100,000+ events/sec` | **1,924,082 events/sec** (250,000 events in `129.9 ms`) | `0.52 µs/event` | PASS |
| **SignalR Market Data Broadcasts** | `1,000+ broadcasts/sec` | **891,297 broadcasts/sec** | `0.02 ms` / `0.02 ms` | PASS |