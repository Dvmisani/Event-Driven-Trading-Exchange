using System.Text.Json;
using Confluent.Kafka;
using TradingExchange.Api.Domain;

namespace TradingExchange.Api.Infrastructure;

public interface IExchangeEventPublisher
{
    Task PublishAsync(IExchangeEvent evt, CancellationToken ct = default);
}

public sealed class KafkaEventPublisher(IConfiguration config) : IExchangeEventPublisher, IDisposable
{
    private readonly string _topic = config["Kafka:Topic"] ?? "exchange.events";
    private readonly IProducer<string, string> _producer = new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:9092",
        Acks = Acks.All,
        EnableIdempotence = true
    }).Build();

    public async Task PublishAsync(IExchangeEvent evt, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize<IExchangeEvent>(evt);
        await _producer.ProduceAsync(_topic, new Message<string, string> { Key = evt.Symbol, Value = json }, ct);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(3));
        _producer.Dispose();
    }
}
