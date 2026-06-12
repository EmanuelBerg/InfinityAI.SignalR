using System.Diagnostics.Metrics;

namespace InfinityAI.SignalR.Services;

public sealed class DockerSignalRMetrics : IDisposable
{
    private readonly Meter _meter = new("InfinityAI.SignalR.Docker", "1.0");

    public Counter<long> MessagesReceived       { get; }
    public Counter<long> MessagesForwarded      { get; }
    public Counter<long> MessagesDiscarded      { get; }
    public Counter<long> ForwardErrors          { get; }
    public Counter<long> SnapshotRequests       { get; }
    public Counter<long> SnapshotSuccess        { get; }
    public Counter<long> SnapshotMissing        { get; }
    public Counter<long> SnapshotSchemaMismatch { get; }

    public DockerSignalRMetrics()
    {
        MessagesReceived       = _meter.CreateCounter<long>("docker.signalr.messages_received",        description: "Total RabbitMQ messages received from signalr.docker.inventory");
        MessagesForwarded      = _meter.CreateCounter<long>("docker.signalr.messages_forwarded",       description: "Total messages forwarded to SignalR docker:services group");
        MessagesDiscarded      = _meter.CreateCounter<long>("docker.signalr.messages_discarded",       description: "Total messages discarded (deserialization error or schema mismatch)");
        ForwardErrors          = _meter.CreateCounter<long>("docker.signalr.forward_errors",           description: "Total SignalR send failures");
        SnapshotRequests       = _meter.CreateCounter<long>("docker.signalr.snapshot_requests",        description: "Total SubscribeDockerServices() calls");
        SnapshotSuccess        = _meter.CreateCounter<long>("docker.signalr.snapshot_success",         description: "Snapshots served from valid Redis payload");
        SnapshotMissing        = _meter.CreateCounter<long>("docker.signalr.snapshot_missing",         description: "Snapshots served as empty (Redis key absent)");
        SnapshotSchemaMismatch = _meter.CreateCounter<long>("docker.signalr.snapshot_schema_mismatch", description: "Snapshots served as schema-mismatch degraded");
    }

    public void Dispose() => _meter.Dispose();
}
