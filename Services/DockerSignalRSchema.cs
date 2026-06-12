namespace InfinityAI.SignalR.Services;

public static class DockerSignalRSchema
{
    public const int ExpectedSchemaVersion = 1;

    // Physical Redis key — must match DockerCacheKeys.StacksAll in InfinityAI.Docker (controlled duplication).
    public const string StacksAllKey = "infinity:docker:stacks:all";

    // SignalR group that all Docker-subscribed connections join.
    public const string DockerServicesGroup = "docker:services";

    // Event names — server → browser.
    public const string EventStackSnapshot = "docker:stack_snapshot";
    public const string EventServiceState  = "docker:service_state";

    // RabbitMQ queue consumed by DockerInventoryConsumer.
    // Declared by InfinityAI.Api RabbitMqTopologyInitializer — this service only consumes.
    public const string ConsumerQueueName = "signalr.docker.inventory";
}
