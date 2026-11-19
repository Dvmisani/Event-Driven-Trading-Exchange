using TradingExchange.Api.Domain;

namespace TradingExchange.Api.Engine;

public sealed class RestingOrder
{
    public required Guid OrderId { get; init; }
    public required string TraderId { get; init; }
    public required OrderSide Side { get; init; }
    public required decimal Price { get; init; }
    public required decimal OriginalQuantity { get; init; }
    public decimal RemainingQuantity { get; set; }
    public required long PlacedSequence { get; init; }
}

public sealed class EventSourcedOrderBook(string symbol)
{
    private readonly Dictionary<Guid, RestingOrder> _orders = [];
    private readonly SortedDictionary<decimal, List<RestingOrder>> _bids = new(Comparer<decimal>.Create((a, b) => b.CompareTo(a)));
    private readonly SortedDictionary<decimal, List<RestingOrder>> _asks = new();
    private readonly List<TradeExecuted> _trades = [];

    public string Symbol { get; } = symbol.ToUpperInvariant();
    public long LastSequenceNumber { get; private set; }
    public IReadOnlyList<TradeExecuted> RecentTrades => _trades;

    public IReadOnlyList<IExchangeEvent> EvaluatePlaceOrder(PlaceOrderCommand cmd)
    {
        if (cmd.Price <= 0) throw new ArgumentException("Price must be positive.");
        if (cmd.Quantity <= 0) throw new ArgumentException("Quantity must be positive.");

        var events = new List<IExchangeEvent>();
        long seq = LastSequenceNumber;
        var now = DateTimeOffset.UtcNow;
        var orderId = Guid.NewGuid();

        events.Add(new OrderPlaced(Guid.NewGuid(), Symbol, ++seq, now, orderId, cmd.TraderId, cmd.Side, cmd.Price, cmd.Quantity));

        decimal remaining = cmd.Quantity;
        var oppositeBook = cmd.Side == OrderSide.Buy ? _asks : _bids;

        foreach (var (bookPrice, queue) in oppositeBook)
        {
            bool crosses = cmd.Side == OrderSide.Buy ? bookPrice <= cmd.Price : bookPrice >= cmd.Price;
            if (!crosses || remaining <= 0) break;

            foreach (var maker in queue)
            {
                if (remaining <= 0) break;
                if (maker.RemainingQuantity <= 0) continue;

                decimal fillQty = Math.Min(remaining, maker.RemainingQuantity);
                remaining -= fillQty;

                events.Add(new OrderMatched(Guid.NewGuid(), Symbol, ++seq, now, maker.OrderId, orderId, maker.Price, fillQty));

                var (buyId, sellId, buyer, seller) = cmd.Side == OrderSide.Buy
                    ? (orderId, maker.OrderId, cmd.TraderId, maker.TraderId)
                    : (maker.OrderId, orderId, maker.TraderId, cmd.TraderId);

                events.Add(new TradeExecuted(Guid.NewGuid(), Symbol, ++seq, now, Guid.NewGuid(), buyId, sellId, buyer, seller, maker.Price, fillQty));
            }
        }

        return events;
    }

    public IReadOnlyList<IExchangeEvent> EvaluateCancelOrder(CancelOrderCommand cmd)
    {
        if (!_orders.TryGetValue(cmd.OrderId, out var order) || order.RemainingQuantity <= 0)
            throw new InvalidOperationException($"Active order {cmd.OrderId} not found.");

        if (!string.Equals(order.TraderId, cmd.TraderId, StringComparison.OrdinalIgnoreCase))
            throw new UnauthorizedAccessException("Trader does not own this order.");

        long seq = LastSequenceNumber + 1;
        return [new OrderCancelled(Guid.NewGuid(), Symbol, seq, DateTimeOffset.UtcNow, order.OrderId, order.TraderId, order.RemainingQuantity)];
    }

    public void Apply(IExchangeEvent evt)
    {
        LastSequenceNumber = evt.SequenceNumber;

        switch (evt)
        {
            case OrderPlaced p:
                var resting = new RestingOrder
                {
                    OrderId = p.OrderId,
                    TraderId = p.TraderId,
                    Side = p.Side,
                    Price = p.Price,
                    OriginalQuantity = p.Quantity,
                    RemainingQuantity = p.Quantity,
                    PlacedSequence = p.SequenceNumber
                };
                _orders[p.OrderId] = resting;
                AddToBook(resting);
                break;

            case OrderMatched m:
                ReduceOrder(m.MakerOrderId, m.MatchedQuantity);
                ReduceOrder(m.TakerOrderId, m.MatchedQuantity);
                break;

            case TradeExecuted t:
                _trades.Add(t);
                if (_trades.Count > 100) _trades.RemoveAt(0);
                break;

            case OrderCancelled c:
                ReduceOrder(c.OrderId, c.RemainingQuantity);
                break;
        }
    }

    public OrderBookSnapshot GetSnapshot()
    {
        var bids = _bids.Select(kv => new OrderBookLevel(kv.Key, kv.Value.Sum(o => o.RemainingQuantity), kv.Value.Count)).ToList();
        var asks = _asks.Select(kv => new OrderBookLevel(kv.Key, kv.Value.Sum(o => o.RemainingQuantity), kv.Value.Count)).ToList();
        return new OrderBookSnapshot(Symbol, LastSequenceNumber, bids, asks);
    }

    private void AddToBook(RestingOrder order)
    {
        var book = order.Side == OrderSide.Buy ? _bids : _asks;
        if (!book.TryGetValue(order.Price, out var list))
        {
            list = [];
            book[order.Price] = list;
        }
        list.Add(order);
    }

    private void ReduceOrder(Guid orderId, decimal qty)
    {
        if (!_orders.TryGetValue(orderId, out var order)) return;
        order.RemainingQuantity -= qty;
        if (order.RemainingQuantity <= 0)
        {
            order.RemainingQuantity = 0;
            var book = order.Side == OrderSide.Buy ? _bids : _asks;
            if (book.TryGetValue(order.Price, out var list))
            {
                list.Remove(order);
                if (list.Count == 0) book.Remove(order.Price);
            }
        }
    }
}
