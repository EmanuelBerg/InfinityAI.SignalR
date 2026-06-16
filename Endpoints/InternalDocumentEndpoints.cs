using InfinityAI.SignalR.Hubs;
using InfinityAI.SignalR.Models;
using Microsoft.AspNetCore.SignalR;

namespace InfinityAI.SignalR.Endpoints;

public static class InternalDocumentEndpoints
{
    public static IEndpointRouteBuilder MapInternalDocumentEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/internal/document");

        group.MapPost("/progress", async (
            DocumentProgressPayload payload,
            IHubContext<ChatHub> hub,
            IConfiguration config,
            ILoggerFactory loggerFactory,
            HttpContext ctx,
            CancellationToken ct) =>
        {
            var logger = loggerFactory.CreateLogger("InfinityAI.SignalR.Internal");
            if (!InternalKeyGuard.IsAuthorized(ctx, config, logger))
                return Results.Unauthorized();

            if (payload.UserId.HasValue)
                await hub.Clients.Group($"chat:user:{payload.UserId}").SendAsync("ReceiveDocumentProgress", payload, ct);

            return Results.Ok();
        });

        return app;
    }
}
