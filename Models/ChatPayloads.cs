namespace InfinityAI.SignalR.Models;

// Reusable progress payload — used for chat prompt processing today,
// extensible to document upload/indexing pipelines in the future.
public sealed record ChatProgressPayload(
    Guid           UserId,
    Guid?          ConversationId,
    Guid           RequestId,
    string         Step,
    string         Message,
    string         Level,
    DateTimeOffset Timestamp);
