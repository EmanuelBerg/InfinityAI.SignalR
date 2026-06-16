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
            ILoggerFactory loggerFactory,
            HttpContext ctx,
            CancellationToken ct) =>
        {
            var logger = loggerFactory.CreateLogger("InfinityAI.SignalR.Internal");
            if (!InternalKeyGuard.IsAuthorized(ctx, config, logger))
                return Results.Unauthorized();

            await hub.Clients.Group($"chat:user:{payload.UserId}").SendAsync("ReceiveChatProgress", payload, ct);
            return Results.Ok();
        });

        return app;
    }
}
