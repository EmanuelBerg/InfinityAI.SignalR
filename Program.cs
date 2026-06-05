using InfinityAI.SignalR.Endpoints;
using InfinityAI.SignalR.Hubs;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.SetMinimumLevel(LogLevel.Information);

builder.Services.AddSignalR();

var app = builder.Build();

var internalKey = app.Configuration["SignalR:InternalKey"];
if (string.IsNullOrWhiteSpace(internalKey))
    app.Logger.LogWarning("[SIGNALR] SignalR:InternalKey is not configured — internal endpoints are open. Set this in production.");

app.UseRouting();

// Live update hubs
app.MapHub<MaintenanceHub>("/hubs/maintenance");
app.MapHub<ChatHub>("/hubs/chat");

// Internal endpoints for API/Worker push
app.MapInternalMaintenanceEndpoints();
app.MapInternalChatEndpoints();
app.MapInternalDocumentEndpoints();

app.Run();
