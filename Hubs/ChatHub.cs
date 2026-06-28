using Microsoft.AspNetCore.SignalR;

namespace InfinityAI.SignalR.Hubs;

// Security model: This hub is an internal Docker Swarm service, not reachable from browsers.
// Only the Blazor Server (infinityai-web) connects via a server-side HubConnection.
// Two layers of protection:
//   1. UserId is read from the connection query string — group membership is set in OnConnectedAsync,
//      so no client code can join an arbitrary group by calling a hub method.
//   2. SignalR:InternalKey is validated on every connection. Connections with a missing or
//      incorrect key are aborted immediately.
public sealed class ChatHub(
    IConfiguration configuration,
    Services.ISecurityEventForwarder securityEvents,
    ILogger<ChatHub> logger) : Hub
{
    public override async Task OnConnectedAsync()
    {
        var httpContext = Context.GetHttpContext();
        var query = httpContext?.Request.Query;

        var requiredKey = configuration["SignalR:InternalKey"];
        if (!string.IsNullOrWhiteSpace(requiredKey))
        {
            var providedKey = query?["internalKey"].ToString();
            if (providedKey != requiredKey)
            {
                logger.LogWarning(
                    "[SIGNALR] ChatHub connection rejected — invalid or missing InternalKey. ConnectionId={ConnectionId}, RemoteIp={Ip}",
                    Context.ConnectionId,
                    httpContext?.Connection.RemoteIpAddress);
                await securityEvents.ForwardAsync("signalr.connection.unauthorized", "ChatHub",
                    Context.ConnectionId, query?["userId"],
                    httpContext?.Connection.RemoteIpAddress?.ToString(),
                    httpContext?.Request.Headers.UserAgent.ToString(),
                    string.IsNullOrEmpty(providedKey) ? "missing internal key" : "invalid internal key");
                Context.Abort();
                return;
            }
        }

        var userId = query?["userId"].ToString();
        if (!string.IsNullOrWhiteSpace(userId))
            await Groups.AddToGroupAsync(Context.ConnectionId, $"chat:user:{userId}");

        await base.OnConnectedAsync();
    }
}
