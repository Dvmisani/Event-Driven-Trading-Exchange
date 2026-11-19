using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using TradingExchange.Api.Domain;
using TradingExchange.Api.Engine;
using TradingExchange.Api.Infrastructure;

namespace TradingExchange.Api.Services;

public sealed class OrderBookStore
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ConcurrentDictionary<string, EventSourcedOrderBook> _books = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _locks = new(StringComparer.OrdinalIgnoreCase);
    private readonly int _snapshotInterval;

    public OrderBookStore(IServiceScopeFactory scopeFactory, IConfiguration config)
    {
        _scopeFactory = scopeFactory;
        _snapshotInterval = config.GetValue<int?>("EventSourcing:SnapshotInterval") ?? 10_000;
    }

    public SemaphoreSlim GetSymbolLock(string symbol) => _locks.GetOrAdd(symbol, _ => new SemaphoreSlim(1, 1));

    public async Task<EventSourcedOrderBook> GetOrLoadAsync(string symbol, CancellationToken ct = default)
    {
        if (_books.TryGetValue(symbol, out var cached)) return cached;
        var book = await LoadFromSnapshotAndTailAsync(symbol, ct);
        _books[symbol] = book;
        return book;
    }

    public async Task<(EventSourcedOrderBook Book, int ReplayedEvents, long? SnapshotSequence)> RebuildFromStoreAsync(string symbol, CancellationToken ct = default)
    {
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<ExchangeDbContext>();

        var latestSnap = await db.OrderBookSnapshots
            .Where(s => s.Symbol == symbol)
            .OrderByDescending(s => s.LastSequenceNumber)
            .FirstOrDefaultAsync(ct);

        var book = new EventSourcedOrderBook(symbol);
        long startSeq = 0;

        if (latestSnap is not null)
        {
            var state = JsonSerializer.Deserialize<OrderBookSnapshotState>(latestSnap.SnapshotJson);
            if (state is not null)
            {
                book.LoadFromSnapshot(state);
                startSeq = state.LastSequenceNumber;
            }
        }

        var tailEntities = await db.Events
            .Where(e => e.Symbol == symbol && e.SequenceNumber > startSeq)
            .OrderBy(e => e.SequenceNumber)
            .ToListAsync(ct);

        foreach (var entity in tailEntities)
        {
            if (DeserializeEvent(entity) is { } evt)
                book.Apply(evt);
        }

        _books[symbol] = book;
        return (book, tailEntities.Count, startSeq > 0 ? startSeq : null);
    }

    public async Task SaveSnapshotIfDueAsync(string symbol, EventSourcedOrderBook book, bool force = false, CancellationToken ct = default)
    {
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<ExchangeDbContext>();

        long lastSnapSeq = await db.OrderBookSnapshots
            .Where(s => s.Symbol == symbol)
            .Select(s => (long?)s.LastSequenceNumber)
            .MaxAsync(ct) ?? 0;

        if (!force && (book.LastSequenceNumber - lastSnapSeq) < _snapshotInterval) return;

        var snapshotState = book.CreateSnapshot();
        db.OrderBookSnapshots.Add(new OrderBookSnapshotEntity
        {
            Symbol = symbol,
            LastSequenceNumber = snapshotState.LastSequenceNumber,
            SnapshotJson = JsonSerializer.Serialize(snapshotState),
            CreatedAtUtc = snapshotState.CapturedAtUtc
        });
        await db.SaveChangesAsync(ct);
    }

    private async Task<EventSourcedOrderBook> LoadFromSnapshotAndTailAsync(string symbol, CancellationToken ct)
    {
        var (book, _, _) = await RebuildFromStoreAsync(symbol, ct);
        return book;
    }

    public static IExchangeEvent? DeserializeEvent(StoredExchangeEvent entity) => entity.EventType switch
    {
        nameof(OrderPlaced) => JsonSerializer.Deserialize<OrderPlaced>(entity.PayloadJson),
        nameof(StopOrderTriggered) => JsonSerializer.Deserialize<StopOrderTriggered>(entity.PayloadJson),
        nameof(OrderMatched) => JsonSerializer.Deserialize<OrderMatched>(entity.PayloadJson),
        nameof(TradeExecuted) => JsonSerializer.Deserialize<TradeExecuted>(entity.PayloadJson),
        nameof(OrderCancelled) => JsonSerializer.Deserialize<OrderCancelled>(entity.PayloadJson),
        nameof(OrderRejected) => JsonSerializer.Deserialize<OrderRejected>(entity.PayloadJson),
        _ => null
    };
}

