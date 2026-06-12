using System.Text.Json;
using InfinityAI.SignalR.Models.Docker;
using StackExchange.Redis;

namespace InfinityAI.SignalR.Services;

public class DockerSignalRCacheReader(
    IConnectionMultiplexer redis,
    ILogger<DockerSignalRCacheReader> logger)
{
    private static readonly JsonSerializerOptions _json = new(JsonSerializerDefaults.Web);

    public sealed record SnapshotResult(
        bool Available,
        bool SchemaMismatch,
        DockerStacksPayload? Payload,
        string Message);

    public async Task<SnapshotResult> ReadStacksSnapshotAsync()
    {
        string? raw;
        try
        {
            raw = await ReadRawValueAsync();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[DOCKER-SIGNALR] Redis read failed for key {Key}", DockerSignalRSchema.StacksAllKey);
            return new SnapshotResult(false, false, null, "Docker inventory is not available yet.");
        }

        if (raw is null)
        {
            logger.LogDebug("[DOCKER-SIGNALR] Redis key {Key} not found", DockerSignalRSchema.StacksAllKey);
            return new SnapshotResult(false, false, null, "Docker inventory is not available yet.");
        }

        DockerStacksPayload? payload;
        try
        {
            payload = JsonSerializer.Deserialize<DockerStacksPayload>(raw, _json);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[DOCKER-SIGNALR] Failed to deserialize DockerStacksPayload from Redis");
            return new SnapshotResult(false, true, null, "Docker inventory schema mismatch.");
        }

        if (payload is null)
            return new SnapshotResult(false, false, null, "Docker inventory is not available yet.");

        if (payload.SchemaVersion != DockerSignalRSchema.ExpectedSchemaVersion)
        {
            logger.LogWarning(
                "[DOCKER-SIGNALR] Redis snapshot schema mismatch: expected {Expected}, got {Actual}",
                DockerSignalRSchema.ExpectedSchemaVersion, payload.SchemaVersion);
            return new SnapshotResult(false, true, null, "Docker inventory schema mismatch.");
        }

        return new SnapshotResult(true, false, payload, "OK");
    }

    // Virtual to allow test subclasses to inject known Redis values without a live connection.
    protected virtual async Task<string?> ReadRawValueAsync()
    {
        var db = redis.GetDatabase();
        var value = await db.StringGetAsync(DockerSignalRSchema.StacksAllKey);
        return value.IsNullOrEmpty ? null : (string?)value;
    }
}
