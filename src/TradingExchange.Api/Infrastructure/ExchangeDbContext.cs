using Microsoft.EntityFrameworkCore;

namespace TradingExchange.Api.Infrastructure;

public sealed class ExchangeDbContext(DbContextOptions<ExchangeDbContext> options) : DbContext(options)
{
    public DbSet<StoredExchangeEvent> Events => Set<StoredExchangeEvent>();
    public DbSet<OrderReadModel> Orders => Set<OrderReadModel>();
    public DbSet<TradeReadModel> Trades => Set<TradeReadModel>();
    public DbSet<OrderBookDepthReadModel> DepthLevels => Set<OrderBookDepthReadModel>();
    public DbSet<ProcessedProjectionEvent> ProcessedEvents => Set<ProcessedProjectionEvent>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<StoredExchangeEvent>().HasIndex(e => e.EventId).IsUnique();
        b.Entity<StoredExchangeEvent>().HasIndex(e => new { e.Symbol, e.SequenceNumber }).IsUnique();
        b.Entity<OrderReadModel>().HasIndex(o => new { o.Symbol, o.Status });
        b.Entity<TradeReadModel>().HasIndex(t => new { t.Symbol, t.ExecutedAtUtc });
        b.Entity<OrderBookDepthReadModel>().HasKey(d => new { d.Symbol, d.Side, d.Price });
        foreach (var p in b.Model.GetEntityTypes().SelectMany(t => t.GetProperties()).Where(p => p.ClrType == typeof(decimal)))
            { p.SetPrecision(18); p.SetScale(4); }
    }
}


