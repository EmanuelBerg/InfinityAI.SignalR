using System.Text;
using System.Text.Json;
using InfinityAI.SignalR.Hubs;
using InfinityAI.SignalR.Models.Docker;
using Microsoft.AspNetCore.SignalR;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace InfinityAI.SignalR.Services;

// Consumes docker.logs → signalr.docker.logs.
// Forwards log lines to docker:logs:{subscriptionId} groups.
public sealed class DockerLogsConsumer(
    IHubContext<DockerHub> hubContext,
    IConfiguration configuration,
    ILogger<DockerLogsConsumer> logger) : BackgroundService
{
    private const string QueueName = DockerSignalRSchema.LogsConsumerQueueName;
    private static readonly JsonSerializerOptions _json = new(JsonSerializerDefaults.Web);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("[DOCKER-LOGS-SIGNALR] Starting on queue={Queue}", QueueName);

        var server  = configuration["RabbitMQServer"] ?? "rabbitmq";
        var port    = int.TryParse(configuration["RabbitMQPort"], out var p) ? p : 5672;
        var factory = new ConnectionFactory { HostName = server, Port = port, AutomaticRecoveryEnabled = true };

        IConnection? connection = null;
        while (!stoppingToken.IsCancellationRequested && connection is null)
        {
            try { connection = await factory.CreateConnectionAsync(stoppingToken); }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning("[DOCKER-LOGS-SIGNALR] RabbitMQ unavailable ({Msg}), retrying in 10s", ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
        }

        if (connection is null) return;
        await using (connection)
        {
            await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
            await channel.BasicQosAsync(0, 50, false, stoppingToken); // higher prefetch for log throughput

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, ea) =>
            {
                var disposition = await HandleReceivedAsync(ea.Body.ToArray(), stoppingToken);
                switch (disposition)
                {
                    case MessageDisposition.Ack:        await channel.BasicAckAsync(ea.DeliveryTag, false); break;
                    case MessageDisposition.NackDiscard: await channel.BasicNackAsync(ea.DeliveryTag, false, false); break;
                    case MessageDisposition.NackRequeue: await channel.BasicNackAsync(ea.DeliveryTag, false, true); break;
                }
            };

            await channel.BasicConsumeAsync(QueueName, autoAck: false, consumer: consumer, stoppingToken);
            logger.LogInformation("[DOCKER-LOGS-SIGNALR] Listening on '{Queue}'", QueueName);
            try { await Task.Delay(Timeout.Infinite, stoppingToken); } catch (OperationCanceledException) { }
        }
    }

    internal async Task<MessageDisposition> HandleReceivedAsync(byte[] body, CancellationToken ct)
    {
        DockerLogLineMessage? msg;
        try { msg = JsonSerializer.Deserialize<DockerLogLineMessage>(Encoding.UTF8.GetString(body), _json); }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[DOCKER-LOGS-SIGNALR] Deserialize failed — discarding");
            return MessageDisposition.NackDiscard;
        }
        if (msg is null || msg.SchemaVersion != DockerSignalRSchema.ExpectedSchemaVersion)
            return MessageDisposition.NackDiscard;

        var group = DockerSignalRSchema.LogsGroupPrefix + msg.SubscriptionId;
        logger.LogDebug("[DOCKER-SIGNALR-LOGS] Received line sub={Sub} svc={Svc} → group={Group}",
            msg.SubscriptionId, msg.ServiceId, group);
        try
        {
            await hubContext.Clients.Group(group).SendAsync(DockerSignalRSchema.EventLogLine, msg, ct);
            logger.LogDebug("[DOCKER-SIGNALR-LOGS] Forwarded line to group {Group}", group);
            return MessageDisposition.Ack;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[DOCKER-LOGS-SIGNALR] Hub send failed — requeueing");
            return MessageDisposition.NackRequeue;
        }
    }
}
