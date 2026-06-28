using System.Net.Http.Json;

namespace InfinityAI.SignalR.Services;

/// <summary>
/// Forwards SignalR connection-security events to the InfinityAI.Api so they land in the unified
/// SecurityAuditEvent store (the SignalR service has no database of its own). Best-effort: never
/// throws and silently no-ops when SecurityAudit:ApiBaseUrl is not configured.
/// </summary>
public interface ISecurityEventForwarder
{
    Task ForwardAsync(string eventType, string hubName, string? connectionId,
        string? userId, string? ip, string? userAgent, string? reason);
}

public sealed class SecurityEventForwarder(
    IHttpClientFactory httpClientFactory,
    IConfiguration config,
    ILogger<SecurityEventForwarder> logger) : ISecurityEventForwarder
{
    public async Task ForwardAsync(string eventType, string hubName, string? connectionId,
        string? userId, string? ip, string? userAgent, string? reason)
    {
        var baseUrl = config["SecurityAudit:ApiBaseUrl"];
        if (string.IsNullOrWhiteSpace(baseUrl)) return; // not configured → disabled

        try
        {
            var client = httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(5);

            using var req = new HttpRequestMessage(HttpMethod.Post,
                $"{baseUrl.TrimEnd('/')}/internal/security/signalr-event");

            var key = config["SignalR:InternalKey"];
            if (!string.IsNullOrWhiteSpace(key))
                req.Headers.Add("X-SignalR-Internal-Key", key);

            req.Content = JsonContent.Create(new
            {
                eventType, hubName, connectionId, userId, ip, userAgent, reason,
            });

            await client.SendAsync(req);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "[SIGNALR-AUDIT] Failed to forward {EventType} for hub {Hub}", eventType, hubName);
        }
    }
}
