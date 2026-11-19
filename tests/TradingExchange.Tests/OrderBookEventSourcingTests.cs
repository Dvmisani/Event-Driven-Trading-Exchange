using TradingExchange.Api.Domain;
using TradingExchange.Api.Engine;
using Xunit;

namespace TradingExchange.Tests;

public sealed class OrderBookEventSourcingTests
{
    [Fact]
    public void MatchingEngine_ExecutesAtMakerPrice_AndRebuildsIdenticalStateFromEvents()
    {
        var book = new EventSourcedOrderBook("JSE:NPN");
        var eventLog = new List<IExchangeEvent>();

        void Execute(IReadOnlyList<IExchangeEvent> events)
        {
            foreach (var e in events)
            {
                eventLog.Add(e);
                book.Apply(e);
            }
        }

        // 1. Resting Bids: BUY 100 @ R102, BUY 200 @ R101
        Execute(book.EvaluatePlaceOrder(new PlaceOrderCommand("JSE:NPN", "trader-alpha", OrderSide.Buy, 102m, 100m)));
        Execute(book.EvaluatePlaceOrder(new PlaceOrderCommand("JSE:NPN", "trader-beta", OrderSide.Buy, 101m, 200m)));

        // 2. Incoming Aggressive Sell: SELL 50 @ R101 -> matches best bid R102 at maker price R102!
        Execute(book.EvaluatePlaceOrder(new PlaceOrderCommand("JSE:NPN", "trader-gamma", OrderSide.Sell, 101m, 50m)));

        // 3. Resting Ask: SELL 100 @ R103 (does not cross R102 bid)
        Execute(book.EvaluatePlaceOrder(new PlaceOrderCommand("JSE:NPN", "trader-delta", OrderSide.Sell, 103m, 100m)));

        var liveSnapshot = book.GetSnapshot();
        Assert.Equal(2, liveSnapshot.Bids.Count);
        Assert.Equal(102m, liveSnapshot.Bids[0].Price);
        Assert.Equal(50m, liveSnapshot.Bids[0].TotalQuantity);
        Assert.Equal(101m, liveSnapshot.Bids[1].Price);
        Assert.Equal(200m, liveSnapshot.Bids[1].TotalQuantity);

        Assert.Single(liveSnapshot.Asks);
        Assert.Equal(103m, liveSnapshot.Asks[0].Price);
        Assert.Equal(100m, liveSnapshot.Asks[0].TotalQuantity);

        var trade = Assert.Single(book.RecentTrades);
        Assert.Equal(102m, trade.Price);
        Assert.Equal(50m, trade.Quantity);

        // 4. Rebuild a brand-new OrderBook purely by replaying the recorded event stream
        var rebuiltBook = new EventSourcedOrderBook("JSE:NPN");
        foreach (var e in eventLog) rebuiltBook.Apply(e);

        var rebuiltSnapshot = rebuiltBook.GetSnapshot();
        Assert.Equal(liveSnapshot.LastSequenceNumber, rebuiltSnapshot.LastSequenceNumber);
        Assert.Equal(liveSnapshot.Bids, rebuiltSnapshot.Bids);
        Assert.Equal(liveSnapshot.Asks, rebuiltSnapshot.Asks);
    }
}
