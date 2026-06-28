using Microsoft.AspNetCore.SignalR;

namespace InfinityAI.SignalR.Hubs;

// Internal-only hub for maintenance job/worker status events.
// Validates SignalR:InternalKey on connection — same pattern as ChatHub.
public sealed class MaintenanceHub(
    IConfiguration configuration,
    Services.ISecurityEventForwarder securityEvents,
    ILogger<MaintenanceHub> logger) : Hub
{
    public override async Task OnConnectedAsync()
    {
        var httpContext = Context.GetHttpContext();

        var requiredKey = configuration["SignalR:InternalKey"];
        if (!string.IsNullOrWhiteSpace(requiredKey))
        {
            var providedKey = httpContext?.Request.Query["internalKey"].ToString();
            if (providedKey != requiredKey)
            {
                logger.LogWarning(
                    "[SIGNALR] MaintenanceHub connection rejected — invalid or missing InternalKey. ConnectionId={ConnectionId}, RemoteIp={Ip}",
                    Context.ConnectionId,
                    httpContext?.Connection.RemoteIpAddress);
                await securityEvents.ForwardAsync("signalr.connection.rejected", "MaintenanceHub",
                    Context.ConnectionId, httpContext?.Request.Query["userId"],
                    httpContext?.Connection.RemoteIpAddress?.ToString(),
                    httpContext?.Request.Headers.UserAgent.ToString(),
                    string.IsNullOrEmpty(providedKey) ? "missing internal key" : "invalid internal key");
                Context.Abort();
                return;
            }
        }

        await base.OnConnectedAsync();
    }
}
