using InfinityAI.SignalR.Hubs;
using InfinityAI.SignalR.Models;
using Microsoft.AspNetCore.SignalR;

namespace InfinityAI.SignalR.Endpoints;

public static class InternalChatEndpoints
{
    public static IEndpointRouteBuilder MapInternalChatEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/internal/chat");

        group.MapPost("/progress", async (
            ChatProgressPayload payload,
            IHubContext<ChatHub> hub,
            IConfiguration config,
            HttpContext ctx,
            CancellationToken ct) =>
        {
            if (!IsAuthorized(ctx, config)) return Results.Unauthorized();

            await hub.Clients.Group($"chat:user:{payload.UserId}").SendAsync("ReceiveChatProgress", payload, ct);
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
