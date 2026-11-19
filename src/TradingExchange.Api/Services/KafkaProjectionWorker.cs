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
    ILogger<KafkaProjectionWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        var topic = config["Kafka:Topic"] ?? "exchange.events";
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
            try
            {
                var cr = consumer.Consume(TimeSpan.FromMilliseconds(250));
                if (cr?.Message?.Value is null) continue;

                var evt = JsonSerializer.Deserialize<IExchangeEvent>(cr.Message.Value);
                if (evt is not null)
                {
                    using var scope = scopeFactory.CreateScope();
                    var db = scope.ServiceProvider.GetRequiredService<ExchangeDbContext>();
                    await ProjectionApplier.ApplyAsync(db, hub, evt, stoppingToken);
                }
                consumer.Commit(cr);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Kafka consumer waiting for broker/topic readiness...");
                await Task.Delay(1000, stoppingToken);
            }
        }
        consumer.Close();
    }
}
