using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
using TradingExchange.Api.Domain;
using TradingExchange.Api.Hubs;
using TradingExchange.Api.Infrastructure;

namespace TradingExchange.Api.Services;

public static class ProjectionApplier
{
    public static async Task ApplyAsync(ExchangeDbContext db, IHubContext<MarketDataHub> hub, IExchangeEvent evt, CancellationToken ct)
    {
        if (await db.ProcessedEvents.AnyAsync(p => p.EventId == evt.EventId, ct)) return;
        db.ProcessedEvents.Add(new ProcessedProjectionEvent { EventId = evt.EventId, ProcessedAtUtc = DateTimeOffset.UtcNow });

        switch (evt)
        {
            case OrderPlaced p:
                db.Orders.Add(new OrderReadModel
                {
                    OrderId = p.OrderId, Symbol = p.Symbol, TraderId = p.TraderId, Side = p.Side,
                    Price = p.Price, OriginalQuantity = p.Quantity, RemainingQuantity = p.Quantity,
                    Status = "Open", UpdatedAtUtc = p.OccurredAtUtc
                });
                await AdjustDepthAsync(db, p.Symbol, p.Side, p.Price, p.Quantity, 1, ct);
                break;

            case OrderMatched m:
                await ReduceOrderAndDepthAsync(db, m.MakerOrderId, m.MatchedQuantity, m.OccurredAtUtc, false, ct);
                await ReduceOrderAndDepthAsync(db, m.TakerOrderId, m.MatchedQuantity, m.OccurredAtUtc, false, ct);
                break;

            case TradeExecuted t:
                db.Trades.Add(new TradeReadModel
                {
                    TradeId = t.TradeId, Symbol = t.Symbol, BuyOrderId = t.BuyOrderId, SellOrderId = t.SellOrderId,
                    BuyerTraderId = t.BuyerTraderId, SellerTraderId = t.SellerTraderId,
                    Price = t.Price, Quantity = t.Quantity, ExecutedAtUtc = t.OccurredAtUtc
                });
                await hub.Clients.Group(t.Symbol).SendAsync("TradeExecuted", t, ct);
                break;

            case OrderCancelled c:
                await ReduceOrderAndDepthAsync(db, c.OrderId, c.RemainingQuantity, c.OccurredAtUtc, true, ct);
                break;
        }

        await db.SaveChangesAsync(ct);

        var levels = await db.DepthLevels.AsNoTracking().Where(d => d.Symbol == evt.Symbol).ToListAsync(ct);
        var bids = levels.Where(l => l.Side == OrderSide.Buy).OrderByDescending(l => l.Price)
            .Select(l => new OrderBookLevel(l.Price, l.TotalQuantity, l.OrderCount)).ToList();
        var asks = levels.Where(l => l.Side == OrderSide.Sell).OrderBy(l => l.Price)
            .Select(l => new OrderBookLevel(l.Price, l.TotalQuantity, l.OrderCount)).ToList();

        await hub.Clients.Group(evt.Symbol).SendAsync("OrderBookUpdated", new OrderBookSnapshot(evt.Symbol, evt.SequenceNumber, bids, asks), ct);
    }

    private static async Task ReduceOrderAndDepthAsync(ExchangeDbContext db, Guid orderId, decimal qty, DateTimeOffset ts, bool cancelled, CancellationToken ct)
    {
        var ord = await db.Orders.FirstOrDefaultAsync(o => o.OrderId == orderId, ct);
        if (ord is null) return;
        ord.RemainingQuantity = Math.Max(0, ord.RemainingQuantity - qty);
        ord.Status = cancelled ? "Cancelled" : (ord.RemainingQuantity == 0 ? "Filled" : "PartiallyFilled");
        ord.UpdatedAtUtc = ts;
        await AdjustDepthAsync(db, ord.Symbol, ord.Side, ord.Price, -qty, ord.RemainingQuantity == 0 ? -1 : 0, ct);
    }

    private static async Task AdjustDepthAsync(ExchangeDbContext db, string symbol, OrderSide side, decimal price, decimal qtyDelta, int countDelta, CancellationToken ct)
    {
        var lvl = await db.DepthLevels.FirstOrDefaultAsync(d => d.Symbol == symbol && d.Side == side && d.Price == price, ct);
        if (lvl is null && qtyDelta > 0)
        {
            db.DepthLevels.Add(new OrderBookDepthReadModel { Symbol = symbol, Side = side, Price = price, TotalQuantity = qtyDelta, OrderCount = Math.Max(1, countDelta) });
            return;
        }
        if (lvl is null) return;
        lvl.TotalQuantity += qtyDelta;
        lvl.OrderCount = Math.Max(0, lvl.OrderCount + countDelta);
        if (lvl.TotalQuantity <= 0) db.DepthLevels.Remove(lvl);
    }
}
