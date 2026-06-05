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
            HttpContext ctx,
            CancellationToken ct) =>
        {
            if (!IsAuthorized(ctx, config)) return Results.Unauthorized();

            if (payload.UserId.HasValue)
                await hub.Clients.Group($"chat:user:{payload.UserId}").SendAsync("ReceiveDocumentProgress", payload, ct);

            return Results.Ok();
        });

        return app;
    }

    private static bool IsAuthorized(HttpContext ctx, IConfiguration config)
    {
        var requiredKey = config["SignalR:InternalKey"];
        if (string.IsNullOrWhiteSpace(requiredKey)) return true;
        ctx.Request.Headers.TryGetValue("X-SignalR-Internal-Key", out var providedKey);
        return providedKey == requiredKey;
    }
}
