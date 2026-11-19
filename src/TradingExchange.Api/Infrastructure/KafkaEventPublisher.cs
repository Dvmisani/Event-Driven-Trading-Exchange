using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using TradingExchange.Api.Domain;
using TradingExchange.Api.Observability;

namespace TradingExchange.Api.Infrastructure;

public interface IExchangeEventPublisher
{
    Task PublishAsync(IExchangeEvent evt, CancellationToken ct = default);
}

public sealed class KafkaEventPublisher : IExchangeEventPublisher, IDisposable
{
    private readonly string _topic;
    private readonly IProducer<string, string> _producer;

    public KafkaEventPublisher(IConfiguration config)
    {
        _topic = config["Kafka:Topic"] ?? "exchange.events";
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:9092",
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();
    }

    public async Task PublishAsync(IExchangeEvent evt, CancellationToken ct = default)
    {
        using var activity = ExchangeTelemetry.ActivitySource.StartActivity("KafkaPublish", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", _topic);
        activity?.SetTag("messaging.kafka.message.key", evt.Symbol);
        activity?.SetTag("exchange.event.type", evt.GetType().Name);

        var headers = new Headers();
        if (Activity.Current?.Id is { } traceParent)
            headers.Add("traceparent", Encoding.UTF8.GetBytes(traceParent));

        var json = JsonSerializer.Serialize<IExchangeEvent>(evt);
        await _producer.ProduceAsync(_topic, new Message<string, string>
        {
            Key = evt.Symbol,
            Value = json,
            Headers = headers
        }, ct);

        ExchangeTelemetry.KafkaMessagesPublished.Add(1,
            new KeyValuePair<string, object?>("symbol", evt.Symbol),
            new KeyValuePair<string, object?>("event_type", evt.GetType().Name));
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(3));
        _producer.Dispose();
    }
}
