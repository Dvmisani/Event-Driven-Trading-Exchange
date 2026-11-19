using Microsoft.AspNetCore.Mvc;
using TradingExchange.Api.Domain;
using TradingExchange.Api.Services;

namespace TradingExchange.Api.Controllers;

[ApiController]
[Route("api/orders")]
public sealed class OrdersController(TradingCommandService commands, OrderBookStore store) : ControllerBase
{
    [HttpPost]
    public async Task<IActionResult> PlaceOrder([FromBody] PlaceOrderCommand cmd, CancellationToken ct)
    {
        var events = await commands.PlaceOrderAsync(cmd, ct);
        var placed = events.OfType<OrderPlaced>().First();
        return Created($"/api/marketdata/{placed.Symbol}/orders/{placed.OrderId}", new
        {
            placed.OrderId,
            placed.Symbol,
            EmittedEvents = events.Select(e => new { Type = e.GetType().Name, e.SequenceNumber, Event = e })
        });
    }

    [HttpDelete("{symbol}/{orderId:guid}")]
    public async Task<IActionResult> CancelOrder(string symbol, Guid orderId, [FromQuery] string traderId, CancellationToken ct)
    {
        var events = await commands.CancelOrderAsync(new CancelOrderCommand(symbol, orderId, traderId), ct);
        return Ok(new { OrderId = orderId, EmittedEvents = events });
    }

    [HttpPost("/api/admin/rebuild/{symbol}")]
    public async Task<IActionResult> RebuildFromEvents(string symbol, CancellationToken ct)
    {
        var rebuilt = await store.GetOrHydrateAsync(symbol, forceRebuild: true, ct);
        return Ok(new { Message = "Rebuilt order book state purely from SQL Server event log.", Snapshot = rebuilt.GetSnapshot() });
    }
}
