using System.Security.Claims;
using System.Text.Encodings.Web;
using InfinityAI.SignalR.Endpoints;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;

namespace InfinityAI.SignalR.Auth;

// Authentication scheme for the InfinityAI.SignalR service.
//
// The SignalR service is an internal Docker service. Two trusted callers reach it:
//   • InfinityAI.Web — connects to the hubs with ?internalKey=<key> in the query string
//     (WebSocket clients cannot set custom headers, so SignalR carries the key as a query param).
//   • InfinityAI.Api / workers — POST to /internal/* push endpoints with the
//     X-SignalR-Internal-Key header (see InternalKeyGuard).
//
// Both prove possession of SignalR:InternalKey. This handler accepts EITHER transport so a single
// FallbackPolicy(RequireAuthenticatedUser) can make the whole service secure-by-default.
//
// Behaviour mirrors the API's InternalKeyAuthenticationHandler:
//   SignalR:InternalKey not configured → permissive dev mode (authenticated, Admin role).
//   Configured + key matches           → authenticated.
//   Configured + missing               → NoResult (anonymous → fallback challenges).
//   Configured + wrong                 → Fail.
public sealed class SignalRInternalKeyAuthenticationHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder,
    IConfiguration config)
    : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    public const string SchemeName = "SignalRInternalKey";
    private const string QueryKeyName = "internalKey";

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var configuredKey = config[InternalKeyGuard.ConfigKey];

        if (string.IsNullOrWhiteSpace(configuredKey))
            return Task.FromResult(AuthenticateResult.Success(BuildTicket("DevMode")));

        // Accept the hub query-string key OR the internal-endpoint header key.
        var provided = Request.Query[QueryKeyName].ToString();
        if (string.IsNullOrEmpty(provided) &&
            Request.Headers.TryGetValue(InternalKeyGuard.HeaderName, out var headerValue))
            provided = headerValue.ToString();

        if (string.IsNullOrEmpty(provided))
            return Task.FromResult(AuthenticateResult.NoResult());

        if (!string.Equals(provided, configuredKey, StringComparison.Ordinal))
        {
            Logger.LogWarning("[SIGNALR-AUTH] Invalid internal key from {RemoteIp}",
                Request.HttpContext.Connection.RemoteIpAddress);
            return Task.FromResult(AuthenticateResult.Fail("Invalid SignalR internal key"));
        }

        return Task.FromResult(AuthenticateResult.Success(BuildTicket("InternalService")));
    }

    private AuthenticationTicket BuildTicket(string name)
    {
        var claims = new[]
        {
            new Claim(ClaimTypes.Name, name),
            new Claim(ClaimTypes.Role, "Admin"),
        };
        var identity = new ClaimsIdentity(claims, SchemeName);
        return new AuthenticationTicket(new ClaimsPrincipal(identity), SchemeName);
    }
}
