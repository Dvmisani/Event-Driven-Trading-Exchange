using System.Text.Json.Serialization;

namespace TradingExchange.Api.Domain;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum OrderSide { Buy, Sell }

public sealed record PlaceOrderCommand(
    string Symbol,
    string TraderId,
    OrderSide Side,
    decimal Price,
    decimal Quantity);

public sealed record CancelOrderCommand(
    string Symbol,
    Guid OrderId,
    string TraderId);

[JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")]
[JsonDerivedType(typeof(OrderPlaced), nameof(OrderPlaced))]
[JsonDerivedType(typeof(OrderMatched), nameof(OrderMatched))]
[JsonDerivedType(typeof(TradeExecuted), nameof(TradeExecuted))]
[JsonDerivedType(typeof(OrderCancelled), nameof(OrderCancelled))]
public interface IExchangeEvent
{
    Guid EventId { get; }
    string Symbol { get; }
    long SequenceNumber { get; }
    DateTimeOffset OccurredAtUtc { get; }
}

public sealed record OrderPlaced(
    Guid EventId,
    string Symbol,
    long SequenceNumber,
    DateTimeOffset OccurredAtUtc,
    Guid OrderId,
    string TraderId,
    OrderSide Side,
    decimal Price,
    decimal Quantity) : IExchangeEvent;

public sealed record OrderMatched(
    Guid EventId,
    string Symbol,
    long SequenceNumber,
    DateTimeOffset OccurredAtUtc,
    Guid MakerOrderId,
    Guid TakerOrderId,
    decimal MatchPrice,
    decimal MatchedQuantity) : IExchangeEvent;

public sealed record TradeExecuted(
    Guid EventId,
    string Symbol,
    long SequenceNumber,
    DateTimeOffset OccurredAtUtc,
    Guid TradeId,
    Guid BuyOrderId,
    Guid SellOrderId,
    string BuyerTraderId,
    string SellerTraderId,
    decimal Price,
    decimal Quantity) : IExchangeEvent;

public sealed record OrderCancelled(
    Guid EventId,
    string Symbol,
    long SequenceNumber,
    DateTimeOffset OccurredAtUtc,
    Guid OrderId,
    string TraderId,
    decimal RemainingQuantity) : IExchangeEvent;

public sealed record OrderBookLevel(decimal Price, decimal TotalQuantity, int OrderCount);

public sealed record OrderBookSnapshot(
    string Symbol,
    long LastSequenceNumber,
    IReadOnlyList<OrderBookLevel> Bids,
    IReadOnlyList<OrderBookLevel> Asks);
