using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TradingExchange.Api.Domain;
using TradingExchange.Api.Infrastructure;

namespace TradingExchange.Api.Controllers;

[ApiController]
[Route("api/marketdata/{symbol}")]
public sealed class MarketDataController(ExchangeDbContext db) : ControllerBase
{
    [HttpGet("orderbook")]
    public async Task<IActionResult> GetOrderBook(string symbol, CancellationToken ct)
    {
        var sym = symbol.Trim().ToUpperInvariant();
        var levels = await db.DepthLevels.AsNoTracking().Where(d => d.Symbol == sym).ToListAsync(ct);
        var lastSeq = await db.Events.AsNoTracking().Where(e => e.Symbol == sym).MaxAsync(e => (long?)e.SequenceNumber, ct) ?? 0;

        var bids = levels.Where(l => l.Side == OrderSide.Buy).OrderByDescending(l => l.Price)
            .Select(l => new OrderBookLevel(l.Price, l.TotalQuantity, l.OrderCount)).ToList();
        var asks = levels.Where(l => l.Side == OrderSide.Sell).OrderBy(l => l.Price)
            .Select(l => new OrderBookLevel(l.Price, l.TotalQuantity, l.OrderCount)).ToList();

        return Ok(new OrderBookSnapshot(sym, lastSeq, bids, asks));
    }

    [HttpGet("trades")]
    public async Task<IActionResult> GetRecentTrades(string symbol, CancellationToken ct)
    {
        var sym = symbol.Trim().ToUpperInvariant();
        var trades = await db.Trades.AsNoTracking()
            .Where(t => t.Symbol == sym)
            .OrderByDescending(t => t.ExecutedAtUtc)
            .Take(50)
            .ToListAsync(ct);
        return Ok(trades);
    }

    [HttpGet("orders/{orderId:guid}")]
    public async Task<IActionResult> GetOrder(string symbol, Guid orderId, CancellationToken ct)
    {
        var sym = symbol.Trim().ToUpperInvariant();
        var order = await db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.Symbol == sym && o.OrderId == orderId, ct);
        return order is null ? NotFound() : Ok(order);
    }

    [HttpGet("events")]
    public async Task<IActionResult> GetEventLog(string symbol, CancellationToken ct)
    {
        var sym = symbol.Trim().ToUpperInvariant();
        var events = await db.Events.AsNoTracking()
            .Where(e => e.Symbol == sym)
            .OrderBy(e => e.SequenceNumber)
            .ToListAsync(ct);
        return Ok(events);
    }
}
