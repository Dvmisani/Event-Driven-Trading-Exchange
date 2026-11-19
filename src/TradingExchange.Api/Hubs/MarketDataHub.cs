using Microsoft.AspNetCore.SignalR;
using TradingExchange.Api.Observability;

namespace TradingExchange.Api.Hubs;

public sealed class MarketDataHub : Hub
{
    public override Task OnConnectedAsync()
    {
        ExchangeTelemetry.SignalRConnections.Add(1);
        return base.OnConnectedAsync();
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        ExchangeTelemetry.SignalRConnections.Add(-1);
        return base.OnDisconnectedAsync(exception);
    }

    public Task SubscribeSymbol(string symbol) =>
        Groups.AddToGroupAsync(Context.ConnectionId, symbol.Trim().ToUpperInvariant());

    public Task UnsubscribeSymbol(string symbol) =>
        Groups.RemoveFromGroupAsync(Context.ConnectionId, symbol.Trim().ToUpperInvariant());
}
