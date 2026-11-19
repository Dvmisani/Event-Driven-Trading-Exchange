using TradingExchange.Api.Domain;
using TradingExchange.Api.Engine;
using TradingExchange.Api.Risk;
using Xunit;

namespace TradingExchange.Tests;

public sealed class OrderBookEventSourcingTests
{
    [Fact]
    public void OrderTypes_Market_IOC_FOK_And_Stop_ExecuteAccordingToExchangeRules()
    {
        var book = new EventSourcedOrderBook("JSE:NPN");

        // Seed resting Ask: 100 @ R101
        foreach (var e in book.EvaluatePlaceOrder(new PlaceOrderCommand("JSE:NPN", "maker-1", OrderSide.Sell, 101m, 100m))) book.Apply(e);

        // 1. FOK for 150 fails because only 100 available
        var fokFail = book.EvaluatePlaceOrder(new PlaceOrderCommand("JSE:NPN", "taker-fok", OrderSide.Buy, 105m, 150m, OrderType.FOK));
        Assert.Contains(fokFail, e => e is OrderCancelled { Reason: "FOK_INSUFFICIENT_LIQUIDITY" });

        // 2. IOC for 60 fills 60 @ R101, leaving 40 resting Ask
        foreach (var e in book.EvaluatePlaceOrder(new PlaceOrderCommand("JSE:NPN", "taker-ioc", OrderSide.Buy, 101m, 60m, OrderType.IOC))) book.Apply(e);
        Assert.Equal(40m, book.ToSnapshotDto().Asks[0].TotalQuantity);

        // 3. Place Buy Stop order at trigger R105 (parks until market reaches R105)
        foreach (var e in book.EvaluatePlaceOrder(new PlaceOrderCommand("JSE:NPN", "stop-buyer", OrderSide.Buy, 106m, 20m, OrderType.Stop, StopPrice: 105m))) book.Apply(e);

        // 4. Add resting Ask @ R105 and execute a trade at R105 -> triggers Stop order automatically
        foreach (var e in book.EvaluatePlaceOrder(new PlaceOrderCommand("JSE:NPN", "maker-2", OrderSide.Sell, 105m, 50m))) book.Apply(e);
        var marketEvts = book.EvaluatePlaceOrder(new PlaceOrderCommand("JSE:NPN", "taker-mkt", OrderSide.Buy, 0m, 45m, OrderType.Market));
        foreach (var e in marketEvts) book.Apply(e);

        Assert.Contains(marketEvts, e => e is StopOrderTriggered { TriggerPrice: 105m });
        Assert.Equal(105m, book.LastTradePrice);
    }

    [Fact]
    public void RiskValidationService_RejectsFatFingerPriceDeviationAndMaxOrderSize()
    {
        var risk = new RiskValidationService();
        var fatFinger = risk.Validate(new PlaceOrderCommand("JSE:NPN", "trader-1", OrderSide.Buy, 200m, 100m), referenceMarketPrice: 100m);
        Assert.False(fatFinger.IsApproved);
        Assert.Equal("FAT_FINGER_PRICE_DEVIATION", fatFinger.RuleCode);

        var oversized = risk.Validate(new PlaceOrderCommand("JSE:NPN", "trader-1", OrderSide.Buy, 101m, 25_000m), referenceMarketPrice: 100m);
        Assert.False(oversized.IsApproved);
        Assert.Equal("MAX_ORDER_SIZE", oversized.RuleCode);
    }

    [Fact]
    public void Snapshot_HydratesBookAndReplaysOnlyTailEvents()
    {
        var primary = new EventSourcedOrderBook("JSE:NPN");
        foreach (var e in primary.EvaluatePlaceOrder(new PlaceOrderCommand("JSE:NPN", "m1", OrderSide.Sell, 102m, 80m))) primary.Apply(e);

        var snapshot = primary.CreateSnapshot();
        var tailEvents = primary.EvaluatePlaceOrder(new PlaceOrderCommand("JSE:NPN", "b1", OrderSide.Buy, 102m, 30m));

        var recovered = new EventSourcedOrderBook("JSE:NPN");
        recovered.LoadFromSnapshot(snapshot);
        foreach (var e in tailEvents) recovered.Apply(e);

        Assert.Equal(50m, recovered.ToSnapshotDto().Asks[0].TotalQuantity);
        Assert.Equal(102m, recovered.LastTradePrice);
    }
}
