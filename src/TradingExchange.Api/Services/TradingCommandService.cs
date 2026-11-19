using System.Text.Json;
using TradingExchange.Api.Domain;
using TradingExchange.Api.Infrastructure;

namespace TradingExchange.Api.Services;

public sealed class TradingCommandService(
    OrderBookStore store,
    IServiceScopeFactory scopeFactory,
    IExchangeEventPublisher publisher)
{
    public async Task<IReadOnlyList<IExchangeEvent>> PlaceOrderAsync(PlaceOrderCommand cmd, CancellationToken ct = default)
    {
        var sym = cmd.Symbol.Trim().ToUpperInvariant();
        var gate = store.GetLock(sym);
        await gate.WaitAsync(ct);
        try
        {
            var book = await store.GetOrHydrateAsync(sym, false, ct);
            var events = book.EvaluatePlaceOrder(cmd with { Symbol = sym });
            await PersistApplyAndPublishAsync(book, events, ct);
            return events;
        }
        finally { gate.Release(); }
    }

    public async Task<IReadOnlyList<IExchangeEvent>> CancelOrderAsync(CancelOrderCommand cmd, CancellationToken ct = default)
    {
        var sym = cmd.Symbol.Trim().ToUpperInvariant();
        var gate = store.GetLock(sym);
        await gate.WaitAsync(ct);
        try
        {
            var book = await store.GetOrHydrateAsync(sym, false, ct);
            var events = book.EvaluateCancelOrder(cmd with { Symbol = sym });
            await PersistApplyAndPublishAsync(book, events, ct);
            return events;
        }
        finally { gate.Release(); }
    }

    private async Task PersistApplyAndPublishAsync(Engine.EventSourcedOrderBook book, IReadOnlyList<IExchangeEvent> events, CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<ExchangeDbContext>();

        foreach (var evt in events)
        {
            db.Events.Add(new StoredExchangeEvent
            {
                EventId = evt.EventId,
                Symbol = evt.Symbol,
                SequenceNumber = evt.SequenceNumber,
                EventType = evt.GetType().Name,
                PayloadJson = JsonSerializer.Serialize<IExchangeEvent>(evt),
                OccurredAtUtc = evt.OccurredAtUtc
            });
        }
        await db.SaveChangesAsync(ct);

        foreach (var evt in events)
        {
            book.Apply(evt);
            await publisher.PublishAsync(evt, ct);
        }
    }
}
