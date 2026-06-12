namespace InfinityAI.SignalR.Models.Docker;

// Unified envelope for the docker:stack_snapshot SignalR event.
// Wraps DockerStacksPayload fields with availability and error metadata
// so clients always receive the same shape regardless of Redis state.
public sealed class DockerStacksSnapshotResponse
{
    public int SchemaVersion { get; init; } = 1;
    public IReadOnlyList<DockerStackDto> Stacks { get; init; } = [];
    public IReadOnlyList<DockerNodeDto> Nodes { get; init; } = [];
    public DockerSwarmOverviewDto? SwarmOverview { get; init; }
    public DateTime? PolledAt { get; init; }
    public bool DockerAvailable { get; init; }
    public bool SchemaMismatch { get; init; }
    public string? Message { get; init; }

    public static DockerStacksSnapshotResponse FromPayload(DockerStacksPayload payload) => new()
    {
        SchemaVersion  = payload.SchemaVersion,
        Stacks         = payload.Stacks,
        Nodes          = payload.Nodes,
        SwarmOverview  = payload.SwarmOverview,
        PolledAt       = payload.PolledAt,
        DockerAvailable = true,
        SchemaMismatch = false
    };

    public static DockerStacksSnapshotResponse Unavailable(string message) => new()
    {
        DockerAvailable = false,
        SchemaMismatch  = false,
        Message         = message
    };

    public static DockerStacksSnapshotResponse SchemaError(string message) => new()
    {
        DockerAvailable = false,
        SchemaMismatch  = true,
        Message         = message
    };
}
