using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using TradingExchange.Api.Domain;
using TradingExchange.Api.Hubs;
using TradingExchange.Api.Infrastructure;
using TradingExchange.Api.Risk;
using TradingExchange.Api.Services;
using Xunit;

namespace TradingExchange.Tests;

public sealed class Phase2ResilienceTests
{
    private sealed class StubEventPublisher : IExchangeEventPublisher
    {
        public List<IExchangeEvent> Published { get; } = new();
        public Task PublishAsync(IExchangeEvent evt, CancellationToken ct = default) { Published.Add(evt); return Task.CompletedTask; }
    }

    private sealed class StubDeadLetterPublisher : IDeadLetterPublisher
    {
        public List<DeadLetterEnvelope> DeadLetters { get; } = new();
        public Task PublishDeadLetterAsync(DeadLetterEnvelope envelope, CancellationToken ct = default) { DeadLetters.Add(envelope); return Task.CompletedTask; }
    }

    private sealed class NoOpHubContext : IHubContext<MarketDataHub>
    {
        public IHubClients Clients => new NoOpClients();
        public IGroupManager Groups => throw new NotImplementedException();
        private sealed class NoOpClients : IHubClients
        {
            public IClientProxy All => new NoOpProxy();
            public IClientProxy AllExcept(IReadOnlyList<string> excludedConnectionIds) => new NoOpProxy();
            public IClientProxy Client(string connectionId) => new NoOpProxy();
            public IClientProxy Clients(IReadOnlyList<string> connectionIds) => new NoOpProxy();
            public IClientProxy Group(string groupName) => new NoOpProxy();
            public IClientProxy GroupExcept(string groupName, IReadOnlyList<string> excludedConnectionIds) => new NoOpProxy();
            public IClientProxy Groups(IReadOnlyList<string> groupNames) => new NoOpProxy();
            public IClientProxy User(string userId) => new NoOpProxy();
            public IClientProxy Users(IReadOnlyList<string> userIds) => new NoOpProxy();
        }
        private sealed class NoOpProxy : IClientProxy
        {
            public Task SendCoreAsync(string method, object?[] args, CancellationToken cancellationToken = default) => Task.CompletedTask;
        }
    }

    [Fact]
    public async Task CommandIdempotency_DuplicateClientRequestId_IgnoresDuplicateExecution()
    {
        var dbName = $"IdempCmd-{Guid.NewGuid()}";
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();
        services.AddSingleton<IConfiguration>(config);
        services.AddDbContext<ExchangeDbContext>(o => o.UseInMemoryDatabase(dbName));
        var provider = services.BuildServiceProvider();

        var publisher = new StubEventPublisher();
        var store = new OrderBookStore(provider.GetRequiredService<IServiceScopeFactory>(), config);
        var cmdService = new TradingCommandService(store, provider.GetRequiredService<IServiceScopeFactory>(), publisher, new RiskValidationService());

        var cmd = new PlaceOrderCommand("AAPL", "trader-1", OrderSide.Buy, 185m, 50m, ClientRequestId: "req-unique-001");

        var first = await cmdService.PlaceOrderAsync(cmd);
        var second = await cmdService.PlaceOrderAsync(cmd);

        Assert.True(first.Accepted);
        Assert.False(first.IsDuplicateReplay);
        Assert.True(second.IsDuplicateReplay);
        Assert.Single(publisher.Published);
        Assert.Equal(first.Events[0].EventId, second.Events[0].EventId);
    }

    [Fact]
    public async Task ProjectionIdempotency_DuplicateKafkaDelivery_DoesNotDoubleCountDepth()
    {
        var options = new DbContextOptionsBuilder<ExchangeDbContext>().UseInMemoryDatabase($"IdempProj-{Guid.NewGuid()}").Options;
        await using var db = new ExchangeDbContext(options);
        var hub = new NoOpHubContext();

        var evt = new OrderPlaced(Guid.NewGuid(), "MSFT", 1, DateTime.UtcNow, Guid.NewGuid(), "trader-1", OrderSide.Buy, 420m, 25m);
        await ProjectionApplier.ApplyAsync(db, hub, evt, CancellationToken.None);
        await ProjectionApplier.ApplyAsync(db, hub, evt, CancellationToken.None);
        await db.SaveChangesAsync();

        var depth = await db.DepthLevels.SingleAsync(d => d.Symbol == "MSFT");
        Assert.Equal(25m, depth.TotalQuantity);
        Assert.Equal(1, depth.OrderCount);
    }

    [Fact]
    public async Task DeadLetterQueue_PoisonMessage_RoutesToDlqAndPersistsAuditRecord()
    {
        var dbName = $"DlqTest-{Guid.NewGuid()}";
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();
        services.AddDbContext<ExchangeDbContext>(o => o.UseInMemoryDatabase(dbName));
        var provider = services.BuildServiceProvider();

        var dlq = new StubDeadLetterPublisher();
        var worker = new KafkaProjectionWorker(
            config,
            provider.GetRequiredService<IServiceScopeFactory>(),
            new NoOpHubContext(),
            dlq,
            NullLogger<KafkaProjectionWorker>.Instance);

        await worker.ProcessRawMessageAsync("exchange.events", 2, 42, "AAPL", "{corrupt-poison-payload", maxRetries: 2, CancellationToken.None);

        Assert.Single(dlq.DeadLetters);
        Assert.Equal("exchange.events", dlq.DeadLetters[0].OriginalTopic);
        Assert.Equal(2, dlq.DeadLetters[0].Partition);
        Assert.Equal(42, dlq.DeadLetters[0].Offset);
        Assert.Equal("JsonException", dlq.DeadLetters[0].ErrorType);
    }
}
