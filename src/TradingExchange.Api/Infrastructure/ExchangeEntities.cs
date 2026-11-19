using System.ComponentModel.DataAnnotations;
using TradingExchange.Api.Domain;

namespace TradingExchange.Api.Infrastructure;

public sealed class StoredExchangeEvent
{
    [Key] public long Id { get; set; }
    public Guid EventId { get; set; }
    [MaxLength(32)] public string Symbol { get; set; } = "";
    public long SequenceNumber { get; set; }
    [MaxLength(64)] public string EventType { get; set; } = "";
    public string PayloadJson { get; set; } = "";
    public DateTimeOffset OccurredAtUtc { get; set; }
}

public sealed class OrderReadModel
{
    [Key] public Guid OrderId { get; set; }
    [MaxLength(32)] public string Symbol { get; set; } = "";
    [MaxLength(64)] public string TraderId { get; set; } = "";
    public OrderSide Side { get; set; }
    public decimal Price { get; set; }
    public decimal OriginalQuantity { get; set; }
    public decimal RemainingQuantity { get; set; }
    [MaxLength(24)] public string Status { get; set; } = "Open";
    public DateTimeOffset UpdatedAtUtc { get; set; }
}

public sealed class TradeReadModel
{
    [Key] public Guid TradeId { get; set; }
    [MaxLength(32)] public string Symbol { get; set; } = "";
    public Guid BuyOrderId { get; set; }
    public Guid SellOrderId { get; set; }
    [MaxLength(64)] public string BuyerTraderId { get; set; } = "";
    [MaxLength(64)] public string SellerTraderId { get; set; } = "";
    public decimal Price { get; set; }
    public decimal Quantity { get; set; }
    public DateTimeOffset ExecutedAtUtc { get; set; }
}

public sealed class OrderBookDepthReadModel
{
    [MaxLength(32)] public string Symbol { get; set; } = "";
    public OrderSide Side { get; set; }
    public decimal Price { get; set; }
    public decimal TotalQuantity { get; set; }
    public int OrderCount { get; set; }
}

public sealed class ProcessedProjectionEvent
{
    [Key] public Guid EventId { get; set; }
    public DateTimeOffset ProcessedAtUtc { get; set; }
}
