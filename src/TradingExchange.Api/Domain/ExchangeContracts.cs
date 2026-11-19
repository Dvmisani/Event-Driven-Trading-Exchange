using System.Text.Json.Serialization;

namespace TradingExchange.Api.Domain;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum OrderSide { Buy, Sell }

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum OrderType { Limit, Market, Stop, IOC, FOK }

public sealed record PlaceOrderCommand(
    string Symbol,
    string TraderId,
    OrderSide Side,
    decimal Price,
    decimal Quantity,
    OrderType Type = OrderType.Limit,
    decimal? StopPrice = null,
    string? ClientRequestId = null);

[JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")]
[JsonDerivedType(typeof(OrderPlaced), nameof(OrderPlaced))]
[JsonDerivedType(typeof(StopOrderTriggered), nameof(StopOrderTriggered))]
[JsonDerivedType(typeof(OrderMatched), nameof(OrderMatched))]
[JsonDerivedType(typeof(TradeExecuted), nameof(TradeExecuted))]
[JsonDerivedType(typeof(OrderCancelled), nameof(OrderCancelled))]
[JsonDerivedType(typeof(OrderRejected), nameof(OrderRejected))]
public interface IExchangeEvent
{
    Guid EventId { get; }
    string Symbol { get; }
    long SequenceNumber { get; }
    DateTime OccurredAtUtc { get; }
}

public sealed record OrderPlaced(
    Guid EventId,
    string Symbol,
    long SequenceNumber,
    DateTime OccurredAtUtc,
    Guid OrderId,
    string TraderId,
    OrderSide Side,
    decimal Price,
    decimal Quantity,
    OrderType Type = OrderType.Limit,
    decimal? StopPrice = null) : IExchangeEvent;

public sealed record StopOrderTriggered(
    Guid EventId,
    string Symbol,
    long SequenceNumber,
    DateTime OccurredAtUtc,
    Guid OrderId,
    decimal TriggerPrice,
    decimal ReferenceMarketPrice) : IExchangeEvent;

public sealed record OrderMatched(
    Guid EventId,
    string Symbol,
    long SequenceNumber,
    DateTime OccurredAtUtc,
    Guid MakerOrderId,
    Guid TakerOrderId,
    decimal MatchedQuantity,
    decimal MatchPrice = 0m) : IExchangeEvent;

public sealed record TradeExecuted(
    Guid EventId,
    string Symbol,
    long SequenceNumber,
    DateTime OccurredAtUtc,
    Guid TradeId,
    Guid BuyOrderId,
    string BuyerTraderId,
    Guid SellOrderId,
    string SellerTraderId,
    decimal Price,
    decimal Quantity) : IExchangeEvent;

public sealed record OrderCancelled(
    Guid EventId,
    string Symbol,
    long SequenceNumber,
    DateTime OccurredAtUtc,
    Guid OrderId,
    decimal RemainingQuantity = 0m,
    string Reason = "USER_CANCELLED") : IExchangeEvent;

public sealed record OrderRejected(
    Guid EventId,
    string Symbol,
    long SequenceNumber,
    DateTime OccurredAtUtc,
    string TraderId,
    string RuleCode,
    string Reason) : IExchangeEvent;

public sealed record RestingOrderState(
    Guid OrderId,
    string TraderId,
    OrderSide Side,
    decimal Price,
    decimal RemainingQuantity,
    long SequencePriority,
    OrderType Type = OrderType.Limit,
    decimal? StopPrice = null);

public sealed record OrderBookSnapshotState(
    string Symbol,
    long LastSequenceNumber,
    decimal? LastTradePrice,
    DateTime CapturedAtUtc,
    List<RestingOrderState> RestingOrders,
    List<RestingOrderState> StopOrders);

public sealed record OrderBookLevel(decimal Price, decimal TotalQuantity, int OrderCount);

public sealed record OrderBookSnapshot(
    string Symbol,
    long SequenceNumber,
    List<OrderBookLevel> Bids,
    List<OrderBookLevel> Asks,
    DateTime GeneratedAtUtc = default,
    decimal? LastTradePrice = null);



