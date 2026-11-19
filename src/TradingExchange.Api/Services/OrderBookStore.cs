using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using TradingExchange.Api.Domain;
using TradingExchange.Api.Engine;
using TradingExchange.Api.Infrastructure;

namespace TradingExchange.Api.Services;

public sealed class OrderBookStore(IServiceScopeFactory scopeFactory)
{
    private readonly ConcurrentDictionary<string, EventSourcedOrderBook> _books = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _locks = new(StringComparer.OrdinalIgnoreCase);

    public SemaphoreSlim GetLock(string symbol) => _locks.GetOrAdd(symbol.Trim().ToUpperInvariant(), _ => new SemaphoreSlim(1, 1));

    public async Task<EventSourcedOrderBook> GetOrHydrateAsync(string symbol, bool forceRebuild = false, CancellationToken ct = default)
    {
        var sym = symbol.Trim().ToUpperInvariant();
        if (!forceRebuild && _books.TryGetValue(sym, out var existing)) return existing;

        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<ExchangeDbContext>();
        var stored = await db.Events.AsNoTracking()
            .Where(e => e.Symbol == sym)
            .OrderBy(e => e.SequenceNumber)
            .ToListAsync(ct);

        var book = new EventSourcedOrderBook(sym);
        foreach (var row in stored)
        {
            var evt = JsonSerializer.Deserialize<IExchangeEvent>(row.PayloadJson)!;
            book.Apply(evt);
        }

        _books[sym] = book;
        return book;
    }
}
