namespace InfinityAI.SignalR.Models;

public sealed record MaintenanceJobUpdatedPayload(
    Guid      JobId,
    string    JobType,
    string    Status,
    DateTime? StartedUtc,
    DateTime? CompletedUtc,
    string?   ResultSummary,
    string?   ErrorMessage,
    DateTime  CreatedUtc);

public sealed record MaintenanceStatusPayload(
    bool      RabbitMqReachable,
    DateTime? WorkerLastSeenUtc,
    string    WorkerStatus,
    int       PendingCount,
    int       RunningCount,
    int       FailedCount,
    DateTime? LastCompletedUtc);
