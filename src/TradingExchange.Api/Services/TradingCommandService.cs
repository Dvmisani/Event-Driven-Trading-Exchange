using System.Diagnostics;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using TradingExchange.Api.Domain;
using TradingExchange.Api.Infrastructure;
using TradingExchange.Api.Observability;
using TradingExchange.Api.Risk;

namespace TradingExchange.Api.Services;

public sealed record OrderCommandResponse(
    bool Accepted,
    string? RejectionRule,
    string? RejectionReason,
    IReadOnlyList<IExchangeEvent> Events,
    bool IsDuplicateReplay = false);

public sealed class TradingCommandService
{
    private readonly OrderBookStore _store;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IExchangeEventPublisher _publisher;
    private readonly RiskValidationService _risk;

    public TradingCommandService(OrderBookStore store, IServiceScopeFactory scopeFactory, IExchangeEventPublisher publisher, RiskValidationService risk)
    {
        _store = store;
        _scopeFactory = scopeFactory;
        _publisher = publisher;
        _risk = risk;
    }

    public async Task<OrderCommandResponse> PlaceOrderAsync(PlaceOrderCommand cmd, CancellationToken ct = default)
    {
        using var submissionSpan = ExchangeTelemetry.ActivitySource.StartActivity("OrderSubmission", ActivityKind.Server);
        submissionSpan?.SetTag("exchange.symbol", cmd.Symbol);
        submissionSpan?.SetTag("exchange.trader_id", cmd.TraderId);
        submissionSpan?.SetTag("exchange.order_type", cmd.Type.ToString());
        submissionSpan?.SetTag("exchange.side", cmd.Side.ToString());

        var symLock = _store.GetSymbolLock(cmd.Symbol);
        await symLock.WaitAsync(ct);
        try
        {
            if (!string.IsNullOrWhiteSpace(cmd.ClientRequestId))
            {
                using var checkScope = _scopeFactory.CreateScope();
                var checkDb = checkScope.ServiceProvider.GetRequiredService<ExchangeDbContext>();
                var existing = await checkDb.IdempotencyRecords
                    .AsNoTracking()
                    .FirstOrDefaultAsync(x => x.ClientRequestId == cmd.ClientRequestId, ct);

                if (existing is not null)
                {
                    var cached = JsonSerializer.Deserialize<OrderCommandResponse>(existing.ResponseJson);
                    if (cached is not null)
                    {
                        submissionSpan?.SetTag("exchange.idempotent_replay", true);
                        return cached with { IsDuplicateReplay = true };
                    }
                }
            }

            var book = await _store.GetOrLoadAsync(cmd.Symbol, ct);
            var riskCheck = _risk.Validate(cmd, book.GetReferencePrice());
            if (!riskCheck.IsApproved)
            {
                ExchangeTelemetry.OrdersSubmitted.Add(1,
                    new KeyValuePair<string, object?>("symbol", cmd.Symbol),
                    new KeyValuePair<string, object?>("status", "rejected"));

                var rejected = new OrderRejected(Guid.NewGuid(), cmd.Symbol, book.LastSequenceNumber + 1, DateTime.UtcNow, cmd.TraderId, riskCheck.RuleCode!, riskCheck.Reason!);
                var rejectedResponse = new OrderCommandResponse(false, riskCheck.RuleCode, riskCheck.Reason, new IExchangeEvent[] { rejected });
                await SaveIdempotencyAsync(cmd, rejectedResponse, ct);
                return rejectedResponse;
            }

            IReadOnlyList<IExchangeEvent> events;
            using (var matchSpan = ExchangeTelemetry.ActivitySource.StartActivity("MatchingEngine", ActivityKind.Internal))
            {
                matchSpan?.SetTag("exchange.symbol", cmd.Symbol);
                var sw = Stopwatch.StartNew();
                events = book.EvaluatePlaceOrder(cmd);
                sw.Stop();
                double elapsedMs = sw.Elapsed.TotalMilliseconds;
                matchSpan?.SetTag("exchange.matching_latency_ms", elapsedMs);
                matchSpan?.SetTag("exchange.emitted_events", events.Count);
                ExchangeTelemetry.MatchingLatencyMs.Record(elapsedMs, new KeyValuePair<string, object?>("symbol", cmd.Symbol));
            }

            _risk.RecordOrderSubmitted(cmd);
            ExchangeTelemetry.OrdersSubmitted.Add(1,
                new KeyValuePair<string, object?>("symbol", cmd.Symbol),
                new KeyValuePair<string, object?>("status", "accepted"));

            var response = new OrderCommandResponse(true, null, null, events);

            using (var scope = _scopeFactory.CreateScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<ExchangeDbContext>();
                foreach (var evt in events)
                {
                    book.Apply(evt);
                    if (evt is TradeExecuted trade)
                    {
                        _risk.RecordTradeFill(trade);
                        ExchangeTelemetry.TradesExecuted.Add(1, new KeyValuePair<string, object?>("symbol", cmd.Symbol));
                    }

                    db.Events.Add(new StoredExchangeEvent
                    {
                        EventId = evt.EventId,
                        Symbol = evt.Symbol,
                        SequenceNumber = evt.SequenceNumber,
                        EventType = evt.GetType().Name,
                        PayloadJson = JsonSerializer.Serialize(evt, evt.GetType()),
                        OccurredAtUtc = evt.OccurredAtUtc
                    });
                }

                if (!string.IsNullOrWhiteSpace(cmd.ClientRequestId))
                {
                    db.IdempotencyRecords.Add(new IdempotencyRecordEntity
                    {
                        ClientRequestId = cmd.ClientRequestId,
                        Symbol = cmd.Symbol,
                        ResponseJson = JsonSerializer.Serialize(response),
                        CreatedAtUtc = DateTime.UtcNow
                    });
                }

                await db.SaveChangesAsync(ct);
            }

            await _store.SaveSnapshotIfDueAsync(cmd.Symbol, book, force: false, ct);

            foreach (var evt in events)
                await _publisher.PublishAsync(evt, ct);

            return response;
        }
        finally { symLock.Release(); }
    }

    public async Task<IReadOnlyList<IExchangeEvent>> CancelOrderAsync(string symbol, Guid orderId, CancellationToken ct = default)
    {
        var symLock = _store.GetSymbolLock(symbol);
        await symLock.WaitAsync(ct);
        try
        {
            var book = await _store.GetOrLoadAsync(symbol, ct);
            var events = book.EvaluateCancelOrder(orderId);

            using (var scope = _scopeFactory.CreateScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<ExchangeDbContext>();
                foreach (var evt in events)
                {
                    book.Apply(evt);
                    db.Events.Add(new StoredExchangeEvent
                    {
                        EventId = evt.EventId,
                        Symbol = evt.Symbol,
                        SequenceNumber = evt.SequenceNumber,
                        EventType = evt.GetType().Name,
                        PayloadJson = JsonSerializer.Serialize(evt, evt.GetType()),
                        OccurredAtUtc = evt.OccurredAtUtc
                    });
                }
                await db.SaveChangesAsync(ct);
            }

            foreach (var evt in events) await _publisher.PublishAsync(evt, ct);
            return events;
        }
        finally { symLock.Release(); }
    }

    private async Task SaveIdempotencyAsync(PlaceOrderCommand cmd, OrderCommandResponse response, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(cmd.ClientRequestId)) return;
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<ExchangeDbContext>();
        db.IdempotencyRecords.Add(new IdempotencyRecordEntity
        {
            ClientRequestId = cmd.ClientRequestId,
            Symbol = cmd.Symbol,
            ResponseJson = JsonSerializer.Serialize(response),
            CreatedAtUtc = DateTime.UtcNow
        });
        await db.SaveChangesAsync(ct);
    }
}
