using Microsoft.AspNetCore.SignalR;

namespace InfinityAI.SignalR.Hubs;

// Internal-only hub for maintenance job/worker status events.
// Validates SignalR:InternalKey on connection — same pattern as ChatHub.
public sealed class MaintenanceHub(IConfiguration configuration, ILogger<MaintenanceHub> logger) : Hub
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
                Context.Abort();
                return;
            }
        }

        await base.OnConnectedAsync();
    }
}
