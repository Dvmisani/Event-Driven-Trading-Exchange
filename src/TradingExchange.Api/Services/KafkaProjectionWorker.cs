using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using TradingExchange.Api.Domain;
using TradingExchange.Api.Hubs;
using TradingExchange.Api.Infrastructure;

namespace TradingExchange.Api.Services;

public sealed class KafkaProjectionWorker(
    IConfiguration config,
    IServiceScopeFactory scopeFactory,
    IHubContext<MarketDataHub> hub,
    IDeadLetterPublisher dlqPublisher,
    ILogger<KafkaProjectionWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        var topic = config["Kafka:Topic"] ?? "exchange.events";
        int maxRetries = config.GetValue<int?>("Kafka:ProjectionMaxRetries") ?? 3;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:9092",
            GroupId = config["Kafka:ConsumerGroup"] ?? "cqrs-read-projector",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(topic);

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, string>? cr = null;
            try
            {
                cr = consumer.Consume(TimeSpan.FromMilliseconds(250));
                if (cr?.Message?.Value is null) continue;
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Kafka consumer waiting for broker/topic readiness...");
                await Task.Delay(1000, stoppingToken);
                continue;
            }

            await ProcessRawMessageAsync(
                cr.Topic,
                cr.Partition.Value,
                cr.Offset.Value,
                cr.Message.Key,
                cr.Message.Value,
                maxRetries,
                stoppingToken);

            consumer.Commit(cr);
        }

        consumer.Close();
    }

    public async Task ProcessRawMessageAsync(
        string topic,
        int partition,
        long offset,
        string? key,
        string rawValue,
        int maxRetries,
        CancellationToken stoppingToken)
    {
        Exception? lastError = null;
        bool succeeded = false;

        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                var evt = JsonSerializer.Deserialize<IExchangeEvent>(rawValue)
                    ?? throw new JsonException("Deserialized exchange event was null.");

                using var scope = scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<ExchangeDbContext>();
                await ProjectionApplier.ApplyAsync(db, hub, evt, stoppingToken);
                succeeded = true;
                break;
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                lastError = ex;
                if (attempt < maxRetries) await Task.Delay(50 * attempt, stoppingToken);
            }
        }

        if (!succeeded && lastError is not null)
        {
            var envelope = new DeadLetterEnvelope(
                topic,
                partition,
                offset,
                key,
                rawValue,
                lastError.GetType().Name,
                lastError.Message,
                maxRetries,
                DateTime.UtcNow);

            logger.LogError(lastError, "Routing poison message on {Topic}[{Partition}]@{Offset} to dead-letter queue.", topic, partition, offset);

            try
            {
                await dlqPublisher.PublishDeadLetterAsync(envelope, stoppingToken);
                using var scope = scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<ExchangeDbContext>();
                db.DeadLetters.Add(new DeadLetterRecordEntity
                {
                    OriginalTopic = envelope.OriginalTopic,
                    Partition = envelope.Partition,
                    Offset = envelope.Offset,
                    MessageKey = envelope.MessageKey,
                    RawPayload = envelope.RawPayload,
                    ErrorType = envelope.ErrorType,
                    ErrorMessage = envelope.ErrorMessage,
                    AttemptCount = envelope.AttemptCount,
                    FailedAtUtc = envelope.FailedAtUtc
                });
                await db.SaveChangesAsync(stoppingToken);
            }
            catch (Exception dlqEx)
            {
                logger.LogCritical(dlqEx, "Failed to persist dead-letter record for offset {Offset}.", offset);
            }
        }
    }
}
