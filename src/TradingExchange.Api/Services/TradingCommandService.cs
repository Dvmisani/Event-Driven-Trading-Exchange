using System.Text.Json;
using TradingExchange.Api.Domain;
using TradingExchange.Api.Infrastructure;
using TradingExchange.Api.Risk;

namespace TradingExchange.Api.Services;

public sealed record OrderCommandResponse(
    bool Accepted,
    string? RejectionRule,
    string? RejectionReason,
    IReadOnlyList<IExchangeEvent> Events);

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
        var symLock = _store.GetSymbolLock(cmd.Symbol);
        await symLock.WaitAsync(ct);
        try
        {
            var book = await _store.GetOrLoadAsync(cmd.Symbol, ct);
            var riskCheck = _risk.Validate(cmd, book.GetReferencePrice());
            if (!riskCheck.IsApproved)
            {
                var rejected = new OrderRejected(Guid.NewGuid(), cmd.Symbol, book.LastSequenceNumber + 1, DateTime.UtcNow, cmd.TraderId, riskCheck.RuleCode!, riskCheck.Reason!);
                return new OrderCommandResponse(false, riskCheck.RuleCode, riskCheck.Reason, new IExchangeEvent[] { rejected });
            }

            var events = book.EvaluatePlaceOrder(cmd);
            _risk.RecordOrderSubmitted(cmd);

            using (var scope = _scopeFactory.CreateScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<ExchangeDbContext>();
                foreach (var evt in events)
                {
                    book.Apply(evt);
                    if (evt is TradeExecuted trade) _risk.RecordTradeFill(trade);

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

            await _store.SaveSnapshotIfDueAsync(cmd.Symbol, book, force: false, ct);

            foreach (var evt in events)
                await _publisher.PublishAsync(evt, ct);

            return new OrderCommandResponse(true, null, null, events);
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
}
