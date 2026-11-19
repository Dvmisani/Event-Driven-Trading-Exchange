using Microsoft.AspNetCore.SignalR;

namespace TradingExchange.Api.Hubs;

public sealed class MarketDataHub : Hub
{
    public Task SubscribeSymbol(string symbol) =>
        Groups.AddToGroupAsync(Context.ConnectionId, symbol.Trim().ToUpperInvariant());

    public Task UnsubscribeSymbol(string symbol) =>
        Groups.RemoveFromGroupAsync(Context.ConnectionId, symbol.Trim().ToUpperInvariant());
}
