using System.Text;
using System.Text.Json;
using InfinityAI.SignalR.Hubs;
using InfinityAI.SignalR.Models.Docker;
using Microsoft.AspNetCore.SignalR;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace InfinityAI.SignalR.Services;

// Consumes docker.progress → signalr.docker.progress.
// Forwards operation progress events to the docker:ops:{operationId} group.
public sealed class DockerProgressConsumer(
    IHubContext<DockerHub> hubContext,
    IConfiguration configuration,
    ILogger<DockerProgressConsumer> logger) : BackgroundService
{
    private const string QueueName = DockerSignalRSchema.ProgressConsumerQueueName;
    private static readonly JsonSerializerOptions _json = new(JsonSerializerDefaults.Web);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("[DOCKER-PROGRESS-SIGNALR] Starting on queue={Queue}", QueueName);

        var server  = configuration["RabbitMQServer"] ?? "rabbitmq";
        var port    = int.TryParse(configuration["RabbitMQPort"], out var p) ? p : 5672;
        var factory = new ConnectionFactory { HostName = server, Port = port, AutomaticRecoveryEnabled = true };

        while (!stoppingToken.IsCancellationRequested)
        {
            // ── Phase 1: establish connection ──────────────────────────────────
            IConnection? connection = null;
            while (!stoppingToken.IsCancellationRequested && connection is null)
            {
                try { connection = await factory.CreateConnectionAsync(stoppingToken); }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogWarning("[DOCKER-PROGRESS-SIGNALR] RabbitMQ unavailable ({Msg}), retrying in 10s", ex.Message);
                    await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
                }
            }
            if (connection is null) return;

            // ── Phase 2: channel + consume loop (retry on missing topology) ────
            await using (connection)
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    IChannel? channel = null;
                    try
                    {
                        channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
                        await channel.BasicQosAsync(0, 10, false, stoppingToken);

                        var consumer = new AsyncEventingBasicConsumer(channel);
                        consumer.ReceivedAsync += async (_, ea) =>
                        {
                            var disposition = await HandleReceivedAsync(ea.Body.ToArray(), stoppingToken);
                            switch (disposition)
                            {
                                case MessageDisposition.Ack:         await channel.BasicAckAsync(ea.DeliveryTag, false); break;
                                case MessageDisposition.NackDiscard: await channel.BasicNackAsync(ea.DeliveryTag, false, false); break;
                                case MessageDisposition.NackRequeue: await channel.BasicNackAsync(ea.DeliveryTag, false, true); break;
                            }
                        };

                        // Throws OperationInterruptedException(404) if queue not declared yet.
                        await channel.BasicConsumeAsync(QueueName, autoAck: false, consumer: consumer, stoppingToken);
                        logger.LogInformation("[DOCKER-PROGRESS-SIGNALR] Listening on '{Queue}'", QueueName);

                        // AutomaticRecovery handles connection drops transparently.
                        await Task.Delay(Timeout.Infinite, stoppingToken);
                    }
                    catch (OperationCanceledException) { return; }
                    catch (Exception ex) when (IsTopologyNotReady(ex))
                    {
                        logger.LogWarning(
                            "[STARTUP-WAIT] RabbitMQ topology not ready; queue {Queue} missing. " +
                            "Waiting for InfinityAI.Api to declare topology.",
                            QueueName);
                        try { await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken); }
                        catch (OperationCanceledException) { return; }
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning("[DOCKER-PROGRESS-SIGNALR] Channel/connection error ({Msg}) — reconnecting", ex.Message);
                        break; // exit inner loop → dispose connection → outer loop reconnects
                    }
                    finally
                    {
                        if (channel is not null)
                            try { await channel.DisposeAsync(); } catch { }
                    }
                }
            }
        }
    }

    internal async Task<MessageDisposition> HandleReceivedAsync(byte[] body, CancellationToken ct)
    {
        DockerOperationProgressMessage? msg;
        try { msg = JsonSerializer.Deserialize<DockerOperationProgressMessage>(Encoding.UTF8.GetString(body), _json); }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[DOCKER-PROGRESS-SIGNALR] Deserialize failed — discarding");
            return MessageDisposition.NackDiscard;
        }
        if (msg is null || msg.SchemaVersion != DockerSignalRSchema.ExpectedSchemaVersion)
            return MessageDisposition.NackDiscard;

        var group     = DockerSignalRSchema.OperationGroupPrefix + msg.OperationId;
        var eventName = msg.Status switch
        {
            "completed" => DockerSignalRSchema.EventOperationCompleted,
            "failed"    => DockerSignalRSchema.EventOperationFailed,
            _           => DockerSignalRSchema.EventOperationProgress
        };

        try
        {
            await hubContext.Clients.Group(group).SendAsync(eventName, msg, ct);
            logger.LogDebug("[DOCKER-PROGRESS-SIGNALR] Forwarded {Event} op={Op} status={Status}",
                eventName, msg.OperationId, msg.Status);
            return MessageDisposition.Ack;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[DOCKER-PROGRESS-SIGNALR] Hub send failed — requeueing");
            return MessageDisposition.NackRequeue;
        }
    }

    // Returns true when the exception indicates that the queue doesn't exist yet —
    // meaning InfinityAI.Api hasn't declared RabbitMQ topology yet.
    internal static bool IsTopologyNotReady(Exception ex) =>
        ex is OperationInterruptedException oie && oie.ShutdownReason?.ReplyCode == 404;
}
