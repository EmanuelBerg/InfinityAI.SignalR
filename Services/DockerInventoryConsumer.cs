using System.Text;
using System.Text.Json;
using InfinityAI.SignalR.Hubs;
using InfinityAI.SignalR.Models.Docker;
using Microsoft.AspNetCore.SignalR;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace InfinityAI.SignalR.Services;

public sealed class DockerInventoryConsumer(
    IHubContext<DockerHub> hubContext,
    DockerSignalRMetrics metrics,
    IConfiguration configuration,
    ILogger<DockerInventoryConsumer> logger) : BackgroundService
{
    // Queue name is centralized in DockerSignalRSchema to keep topology constants testable.
    private const string QueueName = DockerSignalRSchema.ConsumerQueueName;
    private static readonly JsonSerializerOptions _json = new(JsonSerializerDefaults.Web);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("[DOCKER-SIGNALR] DockerInventoryConsumer starting. Queue={Queue}", QueueName);

        var server = configuration["RabbitMQServer"] ?? "rabbitmq";
        var port   = int.TryParse(configuration["RabbitMQPort"], out var p) ? p : 5672;

        logger.LogInformation(
            "[DOCKER-SIGNALR] RabbitMQ: Host={Host} Port={Port} AutomaticRecovery=True",
            server, port);

        var factory = new ConnectionFactory
        {
            HostName                 = server,
            Port                     = port,
            AutomaticRecoveryEnabled = true
        };

        IConnection? connection = null;
        while (!stoppingToken.IsCancellationRequested && connection is null)
        {
            try
            {
                connection = await factory.CreateConnectionAsync(stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning("[DOCKER-SIGNALR] RabbitMQ unavailable ({Msg}), retrying in 10s…", ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
        }

        if (connection is null) return;

        await using (connection)
        {
            await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
            await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 10, global: false, stoppingToken);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, ea) =>
            {
                var disposition = await HandleReceivedAsync(ea.Body.ToArray(), stoppingToken);

                switch (disposition)
                {
                    case MessageDisposition.Ack:
                        await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                        break;
                    case MessageDisposition.NackDiscard:
                        await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                        break;
                    case MessageDisposition.NackRequeue:
                        await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
                        break;
                }
            };

            await channel.BasicConsumeAsync(QueueName, autoAck: false, consumer: consumer, stoppingToken);
            logger.LogInformation("[DOCKER-SIGNALR] Listening on queue '{Queue}'", QueueName);

            try { await Task.Delay(Timeout.Infinite, stoppingToken); }
            catch (OperationCanceledException) { /* normal shutdown */ }
        }

        logger.LogInformation("[DOCKER-SIGNALR] DockerInventoryConsumer stopped.");
    }

    // Internal so InfinityAI.SignalR.Tests can call it directly without a live RabbitMQ connection.
    internal async Task<MessageDisposition> HandleReceivedAsync(byte[] body, CancellationToken ct)
    {
        metrics.MessagesReceived.Add(1);

        DockerServiceStateMessage? msg;
        try
        {
            msg = JsonSerializer.Deserialize<DockerServiceStateMessage>(
                Encoding.UTF8.GetString(body), _json);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[DOCKER-SIGNALR] Failed to deserialize DockerServiceStateMessage — discarding");
            metrics.MessagesDiscarded.Add(1);
            return MessageDisposition.NackDiscard;
        }

        if (msg is null)
        {
            logger.LogWarning("[DOCKER-SIGNALR] Deserialized null message — discarding");
            metrics.MessagesDiscarded.Add(1);
            return MessageDisposition.NackDiscard;
        }

        if (msg.SchemaVersion != DockerSignalRSchema.ExpectedSchemaVersion)
        {
            logger.LogWarning(
                "[DOCKER-SIGNALR] Schema mismatch: expected {Expected}, got {Actual} for service {ServiceName} — discarding",
                DockerSignalRSchema.ExpectedSchemaVersion, msg.SchemaVersion, msg.ServiceName);
            metrics.MessagesDiscarded.Add(1);
            return MessageDisposition.NackDiscard;
        }

        try
        {
            await hubContext.Clients
                .Group(DockerSignalRSchema.DockerServicesGroup)
                .SendAsync(DockerSignalRSchema.EventServiceState, msg, ct);

            metrics.MessagesForwarded.Add(1);
            logger.LogDebug(
                "[DOCKER-SIGNALR] Forwarded {Event}: Service={ServiceName} Stack={StackName} Changes=[{ChangeTypes}] Summary={Summary}",
                DockerSignalRSchema.EventServiceState,
                msg.ServiceName, msg.StackName,
                string.Join(",", msg.ChangeTypes), msg.ChangeSummary);

            return MessageDisposition.Ack;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[DOCKER-SIGNALR] Failed to forward {Event} to SignalR group — requeueing",
                DockerSignalRSchema.EventServiceState);
            metrics.ForwardErrors.Add(1);
            return MessageDisposition.NackRequeue;
        }
    }
}

internal enum MessageDisposition { Ack, NackDiscard, NackRequeue }
