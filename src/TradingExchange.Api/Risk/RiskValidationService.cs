using System.Collections.Concurrent;
using TradingExchange.Api.Domain;

namespace TradingExchange.Api.Risk;

public sealed record RiskLimits(
    decimal MaxOrderSize = 10_000m,
    decimal MaxDailyVolume = 50_000m,
    decimal MaxPositionExposure = 25_000m,
    decimal MaxDailyLoss = 100_000m,
    decimal MaxPriceDeviationPercent = 0.20m);

public sealed record RiskValidationResult(bool IsApproved, string? RuleCode = null, string? Reason = null)
{
    public static RiskValidationResult Approved() => new(true);
    public static RiskValidationResult Rejected(string code, string reason) => new(false, code, reason);
}

public sealed record TraderRiskProfile(
    string TraderId,
    decimal DailyVolume,
    decimal DailyRealizedPnL,
    Dictionary<string, decimal> NetPositions,
    RiskLimits Limits);

public sealed class RiskValidationService
{
    private sealed class TraderLedger
    {
        public DateOnly TradingDay { get; set; } = DateOnly.FromDateTime(DateTime.UtcNow);
        public decimal DailyVolume { get; set; }
        public decimal DailyRealizedPnL { get; set; }
        public Dictionary<string, decimal> Positions { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, decimal> AvgEntryPrice { get; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private readonly ConcurrentDictionary<string, TraderLedger> _ledgers = new(StringComparer.OrdinalIgnoreCase);
    private readonly RiskLimits _limits;

    public RiskValidationService(RiskLimits? limits = null) => _limits = limits ?? new RiskLimits();

    public RiskValidationResult Validate(PlaceOrderCommand cmd, decimal? referenceMarketPrice)
    {
        if (cmd.Quantity <= 0)
            return RiskValidationResult.Rejected("INVALID_QUANTITY", "Order quantity must be positive.");

        if (cmd.Quantity > _limits.MaxOrderSize)
            return RiskValidationResult.Rejected("MAX_ORDER_SIZE", $"Order quantity {cmd.Quantity} exceeds max order size {_limits.MaxOrderSize}.");

        var ledger = GetOrResetToday(cmd.TraderId);
        lock (ledger)
        {
            if (ledger.DailyRealizedPnL <= -_limits.MaxDailyLoss)
                return RiskValidationResult.Rejected("DAILY_LOSS_LIMIT", $"Daily loss {ledger.DailyRealizedPnL:F2} breached limit -{_limits.MaxDailyLoss:F2}.");

            if (ledger.DailyVolume + cmd.Quantity > _limits.MaxDailyVolume)
                return RiskValidationResult.Rejected("MAX_DAILY_VOLUME", $"Projected daily volume {ledger.DailyVolume + cmd.Quantity} exceeds limit {_limits.MaxDailyVolume}.");

            decimal currentPos = ledger.Positions.GetValueOrDefault(cmd.Symbol, 0m);
            decimal signedDelta = cmd.Side == OrderSide.Buy ? cmd.Quantity : -cmd.Quantity;
            decimal projectedPos = currentPos + signedDelta;

            if (Math.Abs(projectedPos) > _limits.MaxPositionExposure)
                return RiskValidationResult.Rejected("MAX_POSITION_EXPOSURE", $"Projected position {projectedPos} exceeds max exposure {_limits.MaxPositionExposure}.");

            if (referenceMarketPrice is > 0 && cmd.Type != OrderType.Market)
            {
                decimal checkPrice = cmd.Type == OrderType.Stop ? (cmd.StopPrice ?? cmd.Price) : cmd.Price;
                decimal deviation = Math.Abs(checkPrice - referenceMarketPrice.Value) / referenceMarketPrice.Value;
                if (deviation > _limits.MaxPriceDeviationPercent)
                    return RiskValidationResult.Rejected(
                        "FAT_FINGER_PRICE_DEVIATION",
                        $"Price {checkPrice} deviates {deviation:P1} from reference market price {referenceMarketPrice.Value} (max {_limits.MaxPriceDeviationPercent:P0}).");
            }
        }

        return RiskValidationResult.Approved();
    }

    public void RecordOrderSubmitted(PlaceOrderCommand cmd)
    {
        var ledger = GetOrResetToday(cmd.TraderId);
        lock (ledger) { ledger.DailyVolume += cmd.Quantity; }
    }

    public void RecordTradeFill(TradeExecuted trade)
    {
        ApplyFill(trade.BuyerTraderId, trade.Symbol, trade.Quantity, trade.Price);
        ApplyFill(trade.SellerTraderId, trade.Symbol, -trade.Quantity, trade.Price);
    }

    public TraderRiskProfile GetProfile(string traderId)
    {
        var ledger = GetOrResetToday(traderId);
        lock (ledger)
        {
            return new TraderRiskProfile(traderId, ledger.DailyVolume, ledger.DailyRealizedPnL, new Dictionary<string, decimal>(ledger.Positions), _limits);
        }
    }

    private void ApplyFill(string traderId, string symbol, decimal signedQty, decimal price)
    {
        var ledger = GetOrResetToday(traderId);
        lock (ledger)
        {
            decimal prevPos = ledger.Positions.GetValueOrDefault(symbol, 0m);
            decimal prevAvg = ledger.AvgEntryPrice.GetValueOrDefault(symbol, price);
            decimal newPos = prevPos + signedQty;

            if (prevPos != 0 && Math.Sign(prevPos) != Math.Sign(signedQty))
            {
                decimal closedQty = Math.Min(Math.Abs(prevPos), Math.Abs(signedQty));
                decimal pnlPerUnit = prevPos > 0 ? (price - prevAvg) : (prevAvg - price);
                ledger.DailyRealizedPnL += closedQty * pnlPerUnit;
            }

            if (newPos == 0) ledger.AvgEntryPrice.Remove(symbol);
            else if (Math.Sign(newPos) != Math.Sign(prevPos)) ledger.AvgEntryPrice[symbol] = price;
            else ledger.AvgEntryPrice[symbol] = ((Math.Abs(prevPos) * prevAvg) + (Math.Abs(signedQty) * price)) / Math.Abs(newPos);

            ledger.Positions[symbol] = newPos;
        }
    }

    private TraderLedger GetOrResetToday(string traderId)
    {
        var ledger = _ledgers.GetOrAdd(traderId, _ => new TraderLedger());
        var today = DateOnly.FromDateTime(DateTime.UtcNow);
        lock (ledger)
        {
            if (ledger.TradingDay != today)
            {
                ledger.TradingDay = today;
                ledger.DailyVolume = 0m;
                ledger.DailyRealizedPnL = 0m;
            }
        }
        return ledger;
    }
}
