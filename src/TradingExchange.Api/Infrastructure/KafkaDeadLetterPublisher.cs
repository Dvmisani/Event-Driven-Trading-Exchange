using System.Text.Json;
using Confluent.Kafka;

namespace TradingExchange.Api.Infrastructure;

public sealed record DeadLetterEnvelope(
    string OriginalTopic,
    int Partition,
    long Offset,
    string? MessageKey,
    string RawPayload,
    string ErrorType,
    string ErrorMessage,
    int AttemptCount,
    DateTime FailedAtUtc);

public interface IDeadLetterPublisher
{
    Task PublishDeadLetterAsync(DeadLetterEnvelope envelope, CancellationToken ct = default);
}

public sealed class KafkaDeadLetterPublisher : IDeadLetterPublisher, IDisposable
{
    private readonly string _dlqTopic;
    private readonly IProducer<string, string> _producer;

    public KafkaDeadLetterPublisher(IConfiguration config)
    {
        _dlqTopic = config["Kafka:DeadLetterTopic"] ?? "exchange.events.deadletter";
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:9092",
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();
    }

    public async Task PublishDeadLetterAsync(DeadLetterEnvelope envelope, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(envelope);
        await _producer.ProduceAsync(_dlqTopic, new Message<string, string>
        {
            Key = envelope.MessageKey ?? "unknown",
            Value = json
        }, ct);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(3));
        _producer.Dispose();
    }
}
