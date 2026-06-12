namespace InfinityAI.SignalR.Models.Docker;

// Controlled duplication of InfinityAI.Docker.Dtos.Commands.DockerLogLineMessage.
public sealed class DockerLogLineMessage
{
    public int SchemaVersion { get; init; } = 1;
    public string SubscriptionId { get; init; } = string.Empty;
    public string ServiceId { get; init; } = string.Empty;
    public string ServiceName { get; init; } = string.Empty;
    public string Source { get; init; } = "stdout";
    public string Line { get; init; } = string.Empty;
    public DateTime OccurredAt { get; init; }
}
