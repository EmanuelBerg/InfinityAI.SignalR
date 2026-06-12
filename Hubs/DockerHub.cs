using InfinityAI.SignalR.Models.Docker;
using InfinityAI.SignalR.Services;
using Microsoft.AspNetCore.SignalR;

namespace InfinityAI.SignalR.Hubs;

// Security model: same pattern as ChatHub/MaintenanceHub.
// Only InfinityAI.Web (Blazor Server) connects via a server-side HubConnection.
// SignalR:InternalKey is validated on every connection; missing/wrong key aborts immediately.
public sealed class DockerHub(
    IConfiguration configuration,
    DockerSignalRCacheReader cacheReader,
    DockerSignalRMetrics metrics,
    ILogger<DockerHub> logger) : Hub
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
                    "[DOCKER-HUB] Connection {ConnectionId} rejected — KeyPresent={KeyPresent} RemoteIp={Ip}",
                    Context.ConnectionId,
                    !string.IsNullOrEmpty(providedKey),
                    httpContext?.Connection.RemoteIpAddress);
                Context.Abort();
                return;
            }
        }

        logger.LogInformation("[DOCKER-HUB] Connection {ConnectionId} accepted", Context.ConnectionId);
        await base.OnConnectedAsync();
    }

    public async Task SubscribeDockerServices()
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, DockerSignalRSchema.DockerServicesGroup);
        logger.LogInformation(
            "[DOCKER-SIGNALR] Connection {ConnectionId} subscribed to {Group}",
            Context.ConnectionId, DockerSignalRSchema.DockerServicesGroup);

        metrics.SnapshotRequests.Add(1);

        var result = await cacheReader.ReadStacksSnapshotAsync();

        DockerStacksSnapshotResponse snapshot;
        if (result.Available && result.Payload is not null)
        {
            metrics.SnapshotSuccess.Add(1);
            snapshot = DockerStacksSnapshotResponse.FromPayload(result.Payload);
        }
        else if (result.SchemaMismatch)
        {
            metrics.SnapshotSchemaMismatch.Add(1);
            snapshot = DockerStacksSnapshotResponse.SchemaError(result.Message);
        }
        else
        {
            metrics.SnapshotMissing.Add(1);
            snapshot = DockerStacksSnapshotResponse.Unavailable(result.Message);
        }

        // Snapshot is sent only to the subscribing caller, not the whole group.
        await Clients.Caller.SendAsync(DockerSignalRSchema.EventStackSnapshot, snapshot);
    }

    public async Task UnsubscribeDockerServices()
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, DockerSignalRSchema.DockerServicesGroup);
        logger.LogInformation(
            "[DOCKER-SIGNALR] Connection {ConnectionId} unsubscribed from {Group}",
            Context.ConnectionId, DockerSignalRSchema.DockerServicesGroup);
    }

    public async Task SubscribeDockerOperation(string operationId)
    {
        var group = DockerSignalRSchema.OperationGroupPrefix + operationId;
        await Groups.AddToGroupAsync(Context.ConnectionId, group);
        logger.LogDebug("[DOCKER-SIGNALR] {ConnectionId} subscribed to {Group}", Context.ConnectionId, group);
    }

    public async Task UnsubscribeDockerOperation(string operationId)
    {
        var group = DockerSignalRSchema.OperationGroupPrefix + operationId;
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, group);
    }

    public async Task SubscribeDockerLogs(string subscriptionId)
    {
        var group = DockerSignalRSchema.LogsGroupPrefix + subscriptionId;
        await Groups.AddToGroupAsync(Context.ConnectionId, group);
        logger.LogInformation("[DOCKER-HUB] {ConnectionId} joined {Group}", Context.ConnectionId, group);
    }

    public async Task UnsubscribeDockerLogs(string subscriptionId)
    {
        var group = DockerSignalRSchema.LogsGroupPrefix + subscriptionId;
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, group);
    }
}
