using TradingExchange.Api.Domain;

namespace TradingExchange.Api.Engine;

public sealed class EventSourcedOrderBook
{
    private sealed class DescendingDecimalComparer : IComparer<decimal>
    {
        public int Compare(decimal x, decimal y) => y.CompareTo(x);
    }

    private readonly SortedDictionary<decimal, Queue<RestingOrderState>> _bids = new(new DescendingDecimalComparer());
    private readonly SortedDictionary<decimal, Queue<RestingOrderState>> _asks = new();
    private readonly List<RestingOrderState> _stopOrders = new();
    private readonly Dictionary<Guid, RestingOrderState> _activeOrders = new();

    public string Symbol { get; }
    public long LastSequenceNumber { get; private set; }
    public decimal? LastTradePrice { get; private set; }

    public EventSourcedOrderBook(string symbol) => Symbol = symbol;

    public decimal? GetReferencePrice()
    {
        if (LastTradePrice.HasValue) return LastTradePrice.Value;
        decimal? bestBid = _bids.Count > 0 ? _bids.Keys.First() : null;
        decimal? bestAsk = _asks.Count > 0 ? _asks.Keys.First() : null;
        if (bestBid.HasValue && bestAsk.HasValue) return (bestBid.Value + bestAsk.Value) / 2m;
        return bestBid ?? bestAsk;
    }

    public OrderBookSnapshotState CreateSnapshot()
    {
        var resting = _bids.Values.SelectMany(q => q)
            .Concat(_asks.Values.SelectMany(q => q))
            .OrderBy(o => o.SequencePriority)
            .ToList();
        return new OrderBookSnapshotState(Symbol, LastSequenceNumber, LastTradePrice, DateTime.UtcNow, resting, _stopOrders.ToList());
    }

    public void LoadFromSnapshot(OrderBookSnapshotState snapshot)
    {
        _bids.Clear();
        _asks.Clear();
        _stopOrders.Clear();
        _activeOrders.Clear();
        LastSequenceNumber = snapshot.LastSequenceNumber;
        LastTradePrice = snapshot.LastTradePrice;

        foreach (var o in snapshot.RestingOrders.OrderBy(x => x.SequencePriority))
            AddResting(o);
        foreach (var s in snapshot.StopOrders.OrderBy(x => x.SequencePriority))
        {
            _stopOrders.Add(s);
            _activeOrders[s.OrderId] = s;
        }
    }

    public void Apply(IExchangeEvent evt)
    {
        LastSequenceNumber = Math.Max(LastSequenceNumber, evt.SequenceNumber);
        switch (evt)
        {
            case OrderPlaced p when p.Type == OrderType.Stop:
                var stop = new RestingOrderState(p.OrderId, p.TraderId, p.Side, p.Price, p.Quantity, p.SequenceNumber, OrderType.Stop, p.StopPrice);
                _stopOrders.Add(stop);
                _activeOrders[p.OrderId] = stop;
                break;
            case OrderPlaced p:
                AddResting(new RestingOrderState(p.OrderId, p.TraderId, p.Side, p.Price, p.Quantity, p.SequenceNumber, p.Type, p.StopPrice));
                break;
            case StopOrderTriggered t:
                var idx = _stopOrders.FindIndex(x => x.OrderId == t.OrderId);
                if (idx >= 0) _stopOrders.RemoveAt(idx);
                break;
            case OrderMatched m:
                if (_activeOrders.TryGetValue(m.MakerOrderId, out var maker))
                    UpdateOrderQuantity(m.MakerOrderId, maker.RemainingQuantity - m.MatchedQuantity);
                if (_activeOrders.TryGetValue(m.TakerOrderId, out var taker))
                    UpdateOrderQuantity(m.TakerOrderId, taker.RemainingQuantity - m.MatchedQuantity);
                break;
            case TradeExecuted tr:
                LastTradePrice = tr.Price;
                break;
            case OrderCancelled c:
                RemoveOrder(c.OrderId);
                break;
        }
    }

    public IReadOnlyList<IExchangeEvent> EvaluatePlaceOrder(PlaceOrderCommand cmd)
    {
        var events = new List<IExchangeEvent>();
        var consumed = new Dictionary<Guid, decimal>();
        var triggeredStops = new HashSet<Guid>();
        long seq = LastSequenceNumber;
        var now = DateTime.UtcNow;
        var orderId = Guid.NewGuid();

        events.Add(new OrderPlaced(Guid.NewGuid(), Symbol, ++seq, now, orderId, cmd.TraderId, cmd.Side, cmd.Price, cmd.Quantity, cmd.Type, cmd.StopPrice));

        if (cmd.Type == OrderType.Stop)
        {
            decimal trigger = cmd.StopPrice ?? cmd.Price;
            if (LastTradePrice.HasValue && IsStopTriggered(cmd.Side, trigger, LastTradePrice.Value))
            {
                triggeredStops.Add(orderId);
                events.Add(new StopOrderTriggered(Guid.NewGuid(), Symbol, ++seq, now, orderId, trigger, LastTradePrice.Value));
                ExecuteAggressive(orderId, cmd.TraderId, cmd.Side, OrderType.Market, cmd.Price, cmd.Quantity, now, ref seq, events, consumed, triggeredStops);
            }
            return events;
        }

        if (cmd.Type == OrderType.FOK && !HasFullFillLiquidity(cmd.Side, cmd.Price, cmd.Quantity, consumed))
        {
            events.Add(new OrderCancelled(Guid.NewGuid(), Symbol, ++seq, now, orderId, cmd.Quantity, "FOK_INSUFFICIENT_LIQUIDITY"));
            return events;
        }

        ExecuteAggressive(orderId, cmd.TraderId, cmd.Side, cmd.Type, cmd.Price, cmd.Quantity, now, ref seq, events, consumed, triggeredStops);
        return events;
    }

    public IReadOnlyList<IExchangeEvent> EvaluateCancelOrder(Guid orderId)
    {
        if (!_activeOrders.TryGetValue(orderId, out var existing)) return Array.Empty<IExchangeEvent>();
        return new IExchangeEvent[] { new OrderCancelled(Guid.NewGuid(), Symbol, LastSequenceNumber + 1, DateTime.UtcNow, orderId, existing.RemainingQuantity, "USER_CANCELLED") };
    }

    private void ExecuteAggressive(
        Guid takerId,
        string takerTrader,
        OrderSide side,
        OrderType type,
        decimal limitPrice,
        decimal qty,
        DateTime now,
        ref long seq,
        List<IExchangeEvent> events,
        Dictionary<Guid, decimal> consumed,
        HashSet<Guid> triggeredStops)
    {
        decimal remaining = qty;
        decimal effectiveLimit = type == OrderType.Market ? (side == OrderSide.Buy ? decimal.MaxValue : 0m) : limitPrice;
        var opposingBook = side == OrderSide.Buy ? _asks : _bids;
        var matchedPrices = new List<decimal>();

        foreach (var (bookPrice, queue) in opposingBook)
        {
            bool crosses = side == OrderSide.Buy ? bookPrice <= effectiveLimit : bookPrice >= effectiveLimit;
            if (!crosses || remaining <= 0) break;

            foreach (var maker in queue)
            {
                if (remaining <= 0) break;
                decimal alreadyUsed = consumed.GetValueOrDefault(maker.OrderId, 0m);
                decimal available = maker.RemainingQuantity - alreadyUsed;
                if (available <= 0) continue;

                decimal fill = Math.Min(remaining, available);
                remaining -= fill;
                consumed[maker.OrderId] = alreadyUsed + fill;

                events.Add(new OrderMatched(Guid.NewGuid(), Symbol, ++seq, now, maker.OrderId, takerId, fill, bookPrice));

                var (buyId, buyer, sellId, seller) = side == OrderSide.Buy
                    ? (takerId, takerTrader, maker.OrderId, maker.TraderId)
                    : (maker.OrderId, maker.TraderId, takerId, takerTrader);

                events.Add(new TradeExecuted(Guid.NewGuid(), Symbol, ++seq, now, Guid.NewGuid(), buyId, buyer, sellId, seller, bookPrice, fill));
                matchedPrices.Add(bookPrice);
            }
        }

        if (remaining > 0 && type is OrderType.IOC or OrderType.Market)
        {
            string reason = type == OrderType.IOC ? "IOC_UNFILLED_REMAINDER" : "MARKET_NO_REMAINING_LIQUIDITY";
            events.Add(new OrderCancelled(Guid.NewGuid(), Symbol, ++seq, now, takerId, remaining, reason));
        }

        foreach (var tradePrice in matchedPrices.Distinct())
            EvaluateTriggeredStops(tradePrice, now, ref seq, events, consumed, triggeredStops);
    }

    private void EvaluateTriggeredStops(
        decimal tradePrice,
        DateTime now,
        ref long seq,
        List<IExchangeEvent> events,
        Dictionary<Guid, decimal> consumed,
        HashSet<Guid> triggeredStops)
    {
        var triggered = _stopOrders
            .Where(s => !triggeredStops.Contains(s.OrderId) && IsStopTriggered(s.Side, s.StopPrice ?? s.Price, tradePrice))
            .OrderBy(s => s.SequencePriority)
            .ToList();

        foreach (var s in triggered)
        {
            triggeredStops.Add(s.OrderId);
            events.Add(new StopOrderTriggered(Guid.NewGuid(), Symbol, ++seq, now, s.OrderId, s.StopPrice ?? s.Price, tradePrice));
            ExecuteAggressive(s.OrderId, s.TraderId, s.Side, OrderType.Market, s.Price, s.RemainingQuantity, now, ref seq, events, consumed, triggeredStops);
        }
    }

    private static bool IsStopTriggered(OrderSide side, decimal stopPrice, decimal marketPrice)
        => side == OrderSide.Buy ? marketPrice >= stopPrice : marketPrice <= stopPrice;

    private bool HasFullFillLiquidity(OrderSide side, decimal limitPrice, decimal requiredQty, Dictionary<Guid, decimal> consumed)
    {
        decimal available = 0m;
        var opposingBook = side == OrderSide.Buy ? _asks : _bids;
        foreach (var (bookPrice, queue) in opposingBook)
        {
            bool crosses = side == OrderSide.Buy ? bookPrice <= limitPrice : bookPrice >= limitPrice;
            if (!crosses) break;
            available += queue.Sum(o => Math.Max(0m, o.RemainingQuantity - consumed.GetValueOrDefault(o.OrderId, 0m)));
            if (available >= requiredQty) return true;
        }
        return false;
    }

    public OrderBookSnapshot ToSnapshotDto()
    {
        var bids = _bids.Select(kv => new OrderBookLevel(kv.Key, kv.Value.Sum(o => o.RemainingQuantity), kv.Value.Count)).ToList();
        var asks = _asks.Select(kv => new OrderBookLevel(kv.Key, kv.Value.Sum(o => o.RemainingQuantity), kv.Value.Count)).ToList();
        return new OrderBookSnapshot(Symbol, LastSequenceNumber, bids, asks, DateTime.UtcNow, LastTradePrice);
    }

    private void AddResting(RestingOrderState order)
    {
        var book = order.Side == OrderSide.Buy ? _bids : _asks;
        if (!book.TryGetValue(order.Price, out var q))
        {
            q = new Queue<RestingOrderState>();
            book[order.Price] = q;
        }
        q.Enqueue(order);
        _activeOrders[order.OrderId] = order;
    }

    private void UpdateOrderQuantity(Guid orderId, decimal remaining)
    {
        if (!_activeOrders.TryGetValue(orderId, out var existing)) return;
        if (remaining <= 0) { RemoveOrder(orderId); return; }

        var updated = existing with { RemainingQuantity = remaining };
        _activeOrders[orderId] = updated;
        var book = existing.Side == OrderSide.Buy ? _bids : _asks;
        if (book.TryGetValue(existing.Price, out var q))
        {
            var items = q.Select(o => o.OrderId == orderId ? updated : o).ToList();
            q.Clear();
            foreach (var item in items) q.Enqueue(item);
        }
    }

    private void RemoveOrder(Guid orderId)
    {
        if (!_activeOrders.Remove(orderId, out var existing)) return;
        var stopIdx = _stopOrders.FindIndex(x => x.OrderId == orderId);
        if (stopIdx >= 0) _stopOrders.RemoveAt(stopIdx);

        var book = existing.Side == OrderSide.Buy ? _bids : _asks;
        if (book.TryGetValue(existing.Price, out var q))
        {
            var items = q.Where(o => o.OrderId != orderId).ToList();
            q.Clear();
            foreach (var item in items) q.Enqueue(item);
            if (q.Count == 0) book.Remove(existing.Price);
        }
    }
}
