using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace TradingExchange.Api.Observability;

public static class ExchangeTelemetry
{
    public const string ServiceName = "TradingExchange.Api";
    public const string ActivitySourceName = "TradingExchange.Engine";
    public const string MeterName = "TradingExchange.Metrics";

    public static readonly ActivitySource ActivitySource = new(ActivitySourceName, "1.0.0");
    public static readonly Meter Meter = new(MeterName, "1.0.0");

    public static readonly Counter<long> OrdersSubmitted =
        Meter.CreateCounter<long>("exchange_orders_submitted_total", "{orders}", "Total orders submitted to the matching engine.");

    public static readonly Counter<long> TradesExecuted =
        Meter.CreateCounter<long>("exchange_trades_executed_total", "{trades}", "Total trades executed by the matching engine.");

    public static readonly Histogram<double> MatchingLatencyMs =
        Meter.CreateHistogram<double>("exchange_matching_latency_ms", "ms", "In-memory matching engine evaluation latency in milliseconds.");

    public static readonly Histogram<double> ProjectionLagMs =
        Meter.CreateHistogram<double>("exchange_projection_lag_ms", "ms", "End-to-end event-to-read-model projection lag in milliseconds.");

    public static readonly Counter<long> KafkaMessagesPublished =
        Meter.CreateCounter<long>("exchange_kafka_messages_published_total", "{messages}", "Total domain events published to Kafka.");

    public static readonly Counter<long> KafkaMessagesConsumed =
        Meter.CreateCounter<long>("exchange_kafka_messages_consumed_total", "{messages}", "Total Kafka messages processed by the projection worker.");

    public static readonly UpDownCounter<long> SignalRConnections =
        Meter.CreateUpDownCounter<long>("exchange_signalr_active_connections", "{connections}", "Current active SignalR WebSocket connections.");
}
