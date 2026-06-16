namespace InfinityAI.SignalR.Endpoints;

/// <summary>
/// Shared authentication guard for all /internal/* push endpoints.
///
/// Contract:
///   Header : X-SignalR-Internal-Key
///   Config : SignalR:InternalKey
///
/// Dev mode (empty/absent config key): requests pass without a header.
/// Production: header value must match the configured key exactly.
/// The configured key is NEVER logged.
/// </summary>
public static class InternalKeyGuard
{
    /// <summary>HTTP header name sent by API/worker callers.</summary>
    public const string HeaderName = "X-SignalR-Internal-Key";

    /// <summary>ASP.NET Core configuration key (colon notation).</summary>
    public const string ConfigKey = "SignalR:InternalKey";

    /// <summary>
    /// Returns true when the request is authorised to call an internal endpoint.
    /// Logs the specific denial reason (missing header / key mismatch) without
    /// revealing the configured key value.
    /// </summary>
    public static bool IsAuthorized(HttpContext ctx, IConfiguration config, ILogger? logger = null)
    {
        var requiredKey = config[ConfigKey];

        // Dev / internal-network mode: no key configured → open.
        // A startup warning is already emitted by Program.cs in this case.
        if (string.IsNullOrWhiteSpace(requiredKey))
            return true;

        if (!ctx.Request.Headers.TryGetValue(HeaderName, out var provided) ||
            string.IsNullOrEmpty(provided))
        {
            logger?.LogWarning(
                "[SIGNALR-AUTH] {Path} denied — {Header} header is missing or empty",
                ctx.Request.Path, HeaderName);
            return false;
        }

        if (provided != requiredKey)
        {
            logger?.LogWarning(
                "[SIGNALR-AUTH] {Path} denied — {Header} present but value does not match configured key",
                ctx.Request.Path, HeaderName);
            return false;
        }

        return true;
    }
}
