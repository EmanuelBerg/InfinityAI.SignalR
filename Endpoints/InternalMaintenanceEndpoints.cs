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
            ILoggerFactory loggerFactory,
            HttpContext ctx,
            CancellationToken ct) =>
        {
            var logger = loggerFactory.CreateLogger("InfinityAI.SignalR.Internal");
            if (!InternalKeyGuard.IsAuthorized(ctx, config, logger))
                return Results.Unauthorized();

            await hub.Clients.All.SendAsync("MaintenanceJobUpdated", payload, ct);
            return Results.Ok();
        });

        group.MapPost("/status-updated", async (
            MaintenanceStatusPayload payload,
            IHubContext<MaintenanceHub> hub,
            IConfiguration config,
            ILoggerFactory loggerFactory,
            HttpContext ctx,
            CancellationToken ct) =>
        {
            var logger = loggerFactory.CreateLogger("InfinityAI.SignalR.Internal");
            if (!InternalKeyGuard.IsAuthorized(ctx, config, logger))
                return Results.Unauthorized();

            await hub.Clients.All.SendAsync("MaintenanceStatusUpdated", payload, ct);
            return Results.Ok();
        });

        return app;
    }
}
