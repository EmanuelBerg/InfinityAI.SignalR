using InfinityAI.SignalR.Hubs;
using InfinityAI.SignalR.Models;
using Microsoft.AspNetCore.SignalR;

namespace InfinityAI.SignalR.Endpoints;

public static class InternalMaintenanceEndpoints
{
    public static IEndpointRouteBuilder MapInternalMaintenanceEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/internal/maintenance");

        group.MapPost("/job-updated", async (
            MaintenanceJobUpdatedPayload payload,
            IHubContext<MaintenanceHub> hub,
            IConfiguration config,
            HttpContext ctx,
            CancellationToken ct) =>
        {
            if (!IsAuthorized(ctx, config)) return Results.Unauthorized();

            await hub.Clients.All.SendAsync("MaintenanceJobUpdated", payload, ct);
            return Results.Ok();
        });

        group.MapPost("/status-updated", async (
            MaintenanceStatusPayload payload,
            IHubContext<MaintenanceHub> hub,
            IConfiguration config,
            HttpContext ctx,
            CancellationToken ct) =>
        {
            if (!IsAuthorized(ctx, config)) return Results.Unauthorized();

            await hub.Clients.All.SendAsync("MaintenanceStatusUpdated", payload, ct);
            return Results.Ok();
        });

        return app;
    }

    private static bool IsAuthorized(HttpContext ctx, IConfiguration config)
    {
        var requiredKey = config["SignalR:InternalKey"];

        // If no key configured: allow (dev/internal-network mode, log warning handled at startup)
        if (string.IsNullOrWhiteSpace(requiredKey)) return true;

        ctx.Request.Headers.TryGetValue("X-SignalR-Internal-Key", out var providedKey);
        return providedKey == requiredKey;
    }
}
