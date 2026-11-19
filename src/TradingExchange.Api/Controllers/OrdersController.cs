using Microsoft.AspNetCore.Mvc;
using TradingExchange.Api.Domain;
using TradingExchange.Api.Risk;
using TradingExchange.Api.Services;

namespace TradingExchange.Api.Controllers;

[ApiController]
[Route("api")]
public sealed class OrdersController : ControllerBase
{
    private readonly TradingCommandService _commands;
    private readonly OrderBookStore _store;
    private readonly RiskValidationService _risk;

    public OrdersController(TradingCommandService commands, OrderBookStore store, RiskValidationService risk)
    {
        _commands = commands;
        _store = store;
        _risk = risk;
    }

    [HttpPost("orders")]
    public async Task<IActionResult> PlaceOrder([FromBody] PlaceOrderCommand cmd, CancellationToken ct)
    {
        var result = await _commands.PlaceOrderAsync(cmd, ct);
        if (!result.Accepted)
            return UnprocessableEntity(new { accepted = false, rule = result.RejectionRule, reason = result.RejectionReason, events = result.Events });
        return Ok(result.Events);
    }

    [HttpDelete("orders/{symbol}/{orderId:guid}")]
    public async Task<IActionResult> CancelOrder(string symbol, Guid orderId, CancellationToken ct)
        => Ok(await _commands.CancelOrderAsync(symbol, orderId, ct));

    [HttpPost("admin/rebuild/{symbol}")]
    public async Task<IActionResult> Rebuild(string symbol, CancellationToken ct)
    {
        var (book, replayed, snapSeq) = await _store.RebuildFromStoreAsync(symbol, ct);
        return Ok(new { symbol, snapshotSequence = snapSeq, replayedTailEvents = replayed, orderBook = book.ToSnapshotDto() });
    }

    [HttpPost("admin/snapshot/{symbol}")]
    public async Task<IActionResult> ForceSnapshot(string symbol, CancellationToken ct)
    {
        var book = await _store.GetOrLoadAsync(symbol, ct);
        await _store.SaveSnapshotIfDueAsync(symbol, book, force: true, ct);
        return Ok(new { symbol, snapshotSequence = book.LastSequenceNumber });
    }

    [HttpGet("risk/{traderId}")]
    public IActionResult GetTraderRisk(string traderId) => Ok(_risk.GetProfile(traderId));
}
