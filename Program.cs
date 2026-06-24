using InfinityAI.SignalR.Auth;
using InfinityAI.SignalR.Endpoints;
using InfinityAI.SignalR.Hubs;
using InfinityAI.SignalR.Services;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.SetMinimumLevel(LogLevel.Information);

builder.Services.AddSignalR();

// SECURE-BY-DEFAULT: authenticate every connection/request with the SignalR internal key
// (query string for hubs, header for /internal push endpoints) and require it via a fallback
// policy. Permissive when SignalR:InternalKey is unset (dev), matching the existing hub checks.
builder.Services
    .AddAuthentication(SignalRInternalKeyAuthenticationHandler.SchemeName)
    .AddScheme<AuthenticationSchemeOptions, SignalRInternalKeyAuthenticationHandler>(
        SignalRInternalKeyAuthenticationHandler.SchemeName, null);

builder.Services.AddAuthorization(options =>
{
    options.FallbackPolicy = new AuthorizationPolicyBuilder()
        .RequireAuthenticatedUser()
        .Build();
});

// Redis — for snapshot-on-subscribe in DockerHub.
var redisConnectionString = builder.Configuration["RedisConnectionString"] ?? "localhost:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var opts = ConfigurationOptions.Parse(redisConnectionString);
    opts.AbortOnConnectFail = false;
    opts.ConnectRetry = 5;
    return ConnectionMultiplexer.Connect(opts);
});

// Docker SignalR services.
builder.Services.AddSingleton<DockerSignalRMetrics>();
builder.Services.AddSingleton<DockerSignalRCacheReader>();
builder.Services.AddHostedService<DockerInventoryConsumer>();
builder.Services.AddHostedService<DockerProgressConsumer>();
builder.Services.AddHostedService<DockerLogsConsumer>();
builder.Services.AddHostedService<DockerHostMetricsConsumer>();

var app = builder.Build();

var internalKey = app.Configuration["SignalR:InternalKey"];
if (string.IsNullOrWhiteSpace(internalKey))
    app.Logger.LogWarning("[SIGNALR] SignalR:InternalKey is not configured — internal endpoints are open. Set this in production.");

app.UseRouting();

app.UseAuthentication();
app.UseAuthorization();

// Live update hubs
app.MapHub<MaintenanceHub>("/hubs/maintenance");
app.MapHub<ChatHub>("/hubs/chat");
app.MapHub<DockerHub>("/hubs/docker");

// Internal endpoints for API/Worker push
app.MapInternalMaintenanceEndpoints();
app.MapInternalChatEndpoints();
app.MapInternalDocumentEndpoints();

// Health check for Docker Swarm healthchecks — must be reachable without the internal key.
app.MapGet("/health/live", () => Results.Ok(new { status = "alive" })).AllowAnonymous();

app.Run();
