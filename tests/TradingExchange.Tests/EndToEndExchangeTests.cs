using System.Net.Http.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.SignalR.Client;
using Testcontainers.Kafka;
using Testcontainers.MsSql;
using TradingExchange.Api.Domain;
using Xunit;

namespace TradingExchange.Tests;

public sealed class EndToEndExchangeTests : IAsyncLifetime
{
    private readonly MsSqlContainer _sql = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2025-latest")
        .WithPassword("Exchange_Strong_Pass123!")
        
        .Build();
    private readonly KafkaContainer _kafka = new KafkaBuilder("confluentinc/cp-kafka:7.6.0").Build();

    public async Task InitializeAsync()
    {
        await _sql.StartAsync();
        await _kafka.StartAsync();
    }

    public async Task DisposeAsync() => await Task.WhenAll(_sql.DisposeAsync().AsTask(), _kafka.DisposeAsync().AsTask());
    [Fact]
    public async Task FullPipeline_PlacesOrders_StreamsOverKafkaAndSignalR_AndRebuildsState()
    {
        await using var factory = new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.UseEnvironment("Development");
            b.UseSetting("ConnectionStrings:DefaultConnection", _sql.GetConnectionString());
            b.UseSetting("Kafka:BootstrapServers", _kafka.GetBootstrapAddress());
            b.UseSetting("Kafka:Topic", "exchange.e2e.events");
            b.UseSetting("Kafka:ConsumerGroup", $"e2e-group-{Guid.NewGuid():N}");
        });

        var client = factory.CreateClient();
        var receivedTrades = new List<TradeExecuted>();

        await using var hub = new HubConnectionBuilder()
            .WithUrl(new Uri(client.BaseAddress!, "/hubs/marketdata"), o => o.HttpMessageHandlerFactory = _ => factory.Server.CreateHandler())
            .Build();
        hub.On<TradeExecuted>("TradeExecuted", t => receivedTrades.Add(t));
        await hub.StartAsync();
        await hub.InvokeAsync("SubscribeSymbol", "JSE:NPN");

        (await client.PostAsJsonAsync("/api/orders", new PlaceOrderCommand("JSE:NPN", "buyer-1", OrderSide.Buy, 102m, 100m))).EnsureSuccessStatusCode();
        (await client.PostAsJsonAsync("/api/orders", new PlaceOrderCommand("JSE:NPN", "seller-1", OrderSide.Sell, 101m, 40m))).EnsureSuccessStatusCode();

        OrderBookSnapshot? book = null;
        for (int i = 0; i < 40; i++)
        {
            book = await client.GetFromJsonAsync<OrderBookSnapshot>("/api/marketdata/JSE:NPN/orderbook");
            if (book is { Bids.Count: 1 } && book.Bids[0].TotalQuantity == 60m && receivedTrades.Count == 1) break;
            await Task.Delay(250);
        }

        Assert.NotNull(book);
        Assert.Equal(60m, book!.Bids[0].TotalQuantity);
        Assert.Equal(102m, Assert.Single(receivedTrades).Price);

        var rebuildRes = await client.PostAsync("/api/admin/rebuild/JSE:NPN", null);
        rebuildRes.EnsureSuccessStatusCode();
    }
}


