namespace InfinityAI.SignalR.Services;

public static class DockerSignalRSchema
{
    public const int ExpectedSchemaVersion = 1;

    // Physical Redis key — must match DockerCacheKeys.StacksAll in InfinityAI.Docker (controlled duplication).
    public const string StacksAllKey = "infinity:docker:stacks:all";

    // SignalR groups
    public const string DockerServicesGroup  = "docker:services";
    public const string OperationGroupPrefix = "docker:ops:";
    public const string LogsGroupPrefix      = "docker:logs:";

    // Events — inventory (server → browser)
    public const string EventStackSnapshot = "docker:stack_snapshot";
    public const string EventServiceState  = "docker:service_state";

    // Events — operations
    public const string EventOperationProgress = "docker:operation_progress";
    public const string EventOperationCompleted = "docker:operation_completed";
    public const string EventOperationFailed    = "docker:operation_failed";

    // Events — logs
    public const string EventLogLine    = "docker:log_line";
    public const string EventLogStopped = "docker:log_stopped";
    public const string EventLogError   = "docker:log_error";

    // SignalR group — host metrics
    public const string DockerHostGroup = "docker:host";

    // Events — host metrics (server → browser)
    public const string EventHostMetrics = "docker:host_metrics";

    // RabbitMQ queues (declared by InfinityAI.Api, consumed here)
    public const string ConsumerQueueName         = "signalr.docker.inventory";
    public const string ProgressConsumerQueueName = "signalr.docker.progress";
    public const string LogsConsumerQueueName     = "signalr.docker.logs";
    public const string HostMetricsQueueName      = "signalr.docker.host";
}
