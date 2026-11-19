using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using TradingExchange.Api.Domain;
using TradingExchange.Api.Infrastructure;
using TradingExchange.Api.Observability;
using TradingExchange.Api.Risk;
using TradingExchange.Api.Services;
using Xunit;

namespace TradingExchange.Tests;

public sealed class Phase3TelemetryTests
{
    private sealed class NoOpPublisher : IExchangeEventPublisher
    {
        public Task PublishAsync(IExchangeEvent evt, CancellationToken ct = default) => Task.CompletedTask;
    }

    [Fact]
    public async Task OpenTelemetry_RecordsOrderSubmissionAndMatchingSpansAndMetrics()
    {
        var recordedSpans = new List<string>();
        using var activityListener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == ExchangeTelemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a => recordedSpans.Add(a.OperationName)
        };
        ActivitySource.AddActivityListener(activityListener);

        long ordersCount = 0;
        long tradesCount = 0;
        using var meterListener = new MeterListener();
        meterListener.InstrumentPublished = (inst, listener) =>
        {
            if (inst.Meter.Name == ExchangeTelemetry.MeterName) listener.EnableMeasurementEvents(inst);
        };
        meterListener.SetMeasurementEventCallback<long>((inst, val, _, _) =>
        {
            if (inst.Name == "exchange_orders_submitted_total") ordersCount += val;
            if (inst.Name == "exchange_trades_executed_total") tradesCount += val;
        });
        meterListener.Start();

        var dbName = $"OtelTest-{Guid.NewGuid()}";
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();
        services.AddSingleton<IConfiguration>(config);
        services.AddDbContext<ExchangeDbContext>(o => o.UseInMemoryDatabase(dbName));
        var provider = services.BuildServiceProvider();

        var store = new OrderBookStore(provider.GetRequiredService<IServiceScopeFactory>(), config);
        var cmdService = new TradingCommandService(store, provider.GetRequiredService<IServiceScopeFactory>(), new NoOpPublisher(), new RiskValidationService());

        await cmdService.PlaceOrderAsync(new PlaceOrderCommand("BTCUSD", "maker", OrderSide.Sell, 95_000m, 1m));
        await cmdService.PlaceOrderAsync(new PlaceOrderCommand("BTCUSD", "taker", OrderSide.Buy, 95_000m, 1m));

        Assert.Contains("OrderSubmission", recordedSpans);
        Assert.Contains("MatchingEngine", recordedSpans);
        Assert.True(ordersCount >= 2);
        Assert.True(tradesCount >= 1);
    }
}
