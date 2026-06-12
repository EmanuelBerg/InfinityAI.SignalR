namespace InfinityAI.SignalR.Models.Docker;

// Controlled duplication of InfinityAI.Docker.Dtos.Commands.DockerOperationProgressMessage.
public sealed class DockerOperationProgressMessage
{
    public int SchemaVersion { get; init; } = 1;
    public Guid OperationId { get; init; }
    public string CommandType { get; init; } = string.Empty;
    public string ServiceId { get; init; } = string.Empty;
    public string ServiceName { get; init; } = string.Empty;
    public string Status { get; init; } = string.Empty;
    public string Step { get; init; } = string.Empty;
    public int ProgressPercent { get; init; }
    public string? Message { get; init; }
    public string? ErrorMessage { get; init; }
    public DateTime OccurredAt { get; init; }
}
