using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TradingExchange.Api.Domain;
using TradingExchange.Api.Infrastructure;
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
    private readonly ExchangeDbContext _db;

    public OrdersController(TradingCommandService commands, OrderBookStore store, RiskValidationService risk, ExchangeDbContext db)
    {
        _commands = commands;
        _store = store;
        _risk = risk;
        _db = db;
    }

    [HttpPost("orders")]
    public async Task<IActionResult> PlaceOrder([FromBody] PlaceOrderCommand cmd, [FromHeader(Name = "X-Idempotency-Key")] string? idempotencyHeader, CancellationToken ct)
    {
        var effectiveCmd = string.IsNullOrWhiteSpace(cmd.ClientRequestId) && !string.IsNullOrWhiteSpace(idempotencyHeader)
            ? cmd with { ClientRequestId = idempotencyHeader }
            : cmd;

        var result = await _commands.PlaceOrderAsync(effectiveCmd, ct);
        if (result.IsDuplicateReplay) Response.Headers["X-Idempotency-Replay"] = "true";

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

    [HttpGet("admin/deadletters")]
    public async Task<IActionResult> GetDeadLetters(CancellationToken ct)
        => Ok(await _db.DeadLetters.AsNoTracking().OrderByDescending(d => d.Id).Take(100).ToListAsync(ct));

    [HttpGet("risk/{traderId}")]
    public IActionResult GetTraderRisk(string traderId) => Ok(_risk.GetProfile(traderId));
}
