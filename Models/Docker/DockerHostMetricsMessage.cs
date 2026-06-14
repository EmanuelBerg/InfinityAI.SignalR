namespace InfinityAI.SignalR.Models.Docker;

// Controlled duplication of InfinityAI.Docker.Dtos.DockerHostMetricsDto.
// Only the fields forwarded via the docker:host_metrics SignalR event are included here.
public sealed class DockerHostMetricsMessage
{
    public int SchemaVersion { get; init; } = 1;
    public DateTime CollectedAt { get; init; }
    public DockerHostCpuMsg Cpu { get; init; } = new();
    public DockerHostMemoryMsg Memory { get; init; } = new();
    public DockerHostDiskMsg Disk { get; init; } = new();
    public DockerHostLoadMsg Load { get; init; } = new();
    public DockerHostDockerCountsMsg DockerCounts { get; init; } = new();
    public int HostHealthScore { get; init; }
    public string HostHealthStatus { get; init; } = "unknown";
    public IReadOnlyList<string> HealthFlags { get; init; } = [];
}

public sealed class DockerHostCpuMsg
{
    public double UsagePercent { get; init; } = -1;
    public int CoreCount { get; init; }
}

public sealed class DockerHostMemoryMsg
{
    public long TotalBytes { get; init; }
    public long UsedBytes { get; init; }
    public long FreeBytes { get; init; }
    public double UsagePercent { get; init; } = -1;
}

public sealed class DockerHostDiskMsg
{
    public long TotalBytes { get; init; }
    public long UsedBytes { get; init; }
    public long FreeBytes { get; init; }
    public double UsagePercent { get; init; } = -1;
    public string MountPoint { get; init; } = string.Empty;
}

public sealed class DockerHostLoadMsg
{
    public double Load1 { get; init; } = -1;
    public double Load5 { get; init; } = -1;
    public double Load15 { get; init; } = -1;
}

public sealed class DockerHostDockerCountsMsg
{
    public int TotalImages { get; init; }
    public long TotalImageBytes { get; init; }
    public int TotalContainers { get; init; }
    public int RunningContainers { get; init; }
    public int StoppedContainers { get; init; }
    public int TotalVolumes { get; init; }
    public int TotalNetworks { get; init; }
}
