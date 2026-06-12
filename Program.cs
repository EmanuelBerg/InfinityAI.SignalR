using InfinityAI.SignalR.Endpoints;
using InfinityAI.SignalR.Hubs;
using InfinityAI.SignalR.Services;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.SetMinimumLevel(LogLevel.Information);

builder.Services.AddSignalR();

// Redis — for snapshot-on-subscribe in DockerHub.
var redisConnectionString = builder.Configuration["RedisConnectionString"] ?? "localhost:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(
    ConnectionMultiplexer.Connect(redisConnectionString));

// Docker SignalR services.
builder.Services.AddSingleton<DockerSignalRMetrics>();
builder.Services.AddSingleton<DockerSignalRCacheReader>();
builder.Services.AddHostedService<DockerInventoryConsumer>();
builder.Services.AddHostedService<DockerProgressConsumer>();
builder.Services.AddHostedService<DockerLogsConsumer>();

var app = builder.Build();

var internalKey = app.Configuration["SignalR:InternalKey"];
if (string.IsNullOrWhiteSpace(internalKey))
    app.Logger.LogWarning("[SIGNALR] SignalR:InternalKey is not configured — internal endpoints are open. Set this in production.");

app.UseRouting();

// Live update hubs
app.MapHub<MaintenanceHub>("/hubs/maintenance");
app.MapHub<ChatHub>("/hubs/chat");
app.MapHub<DockerHub>("/hubs/docker");

// Internal endpoints for API/Worker push
app.MapInternalMaintenanceEndpoints();
app.MapInternalChatEndpoints();
app.MapInternalDocumentEndpoints();

app.Run();
