namespace InfinityAI.SignalR.Models;

public sealed record DocumentProgressPayload(
    Guid?          UserId,
    Guid           DocumentId,
    Guid?          FileId,
    string         FileName,
    string         Status,
    string?        Stage,
    int?           Current,
    int?           Total,
    double?        Percent,
    string?        ErrorMessage,
    DateTimeOffset Timestamp);
