using System.Text;
using System.Text.Json;
using InfinityAI.SignalR.Hubs;
using InfinityAI.SignalR.Models.Docker;
using Microsoft.AspNetCore.SignalR;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace InfinityAI.SignalR.Services;

public sealed class DockerHostMetricsConsumer(
    IHubContext<DockerHub> hubContext,
    IConfiguration configuration,
    ILogger<DockerHostMetricsConsumer> logger) : BackgroundService
{
    private const string QueueName = DockerSignalRSchema.HostMetricsQueueName;
    private static readonly JsonSerializerOptions _json = new(JsonSerializerDefaults.Web);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("[DOCKER-HOST] DockerHostMetricsConsumer starting. Queue={Queue}", QueueName);

        var server = configuration["RabbitMQServer"] ?? "rabbitmq";
        var port   = int.TryParse(configuration["RabbitMQPort"], out var p) ? p : 5672;

        var factory = new ConnectionFactory
        {
            HostName                 = server,
            Port                     = port,
            AutomaticRecoveryEnabled = true
        };

        while (!stoppingToken.IsCancellationRequested)
        {
            IConnection? connection = null;
            while (!stoppingToken.IsCancellationRequested && connection is null)
            {
                try
                {
                    connection = await factory.CreateConnectionAsync(stoppingToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogWarning("[DOCKER-HOST] RabbitMQ unavailable ({Msg}), retrying in 10s…", ex.Message);
                    await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
                }
            }
            if (connection is null) return;

            await using (connection)
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    IChannel? channel = null;
                    try
                    {
                        channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
                        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 5, global: false, stoppingToken);

                        var consumer = new AsyncEventingBasicConsumer(channel);
                        consumer.ReceivedAsync += async (_, ea) =>
                        {
                            var ok = await HandleReceivedAsync(ea.Body.ToArray(), stoppingToken);
                            if (ok)
                                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                            else
                                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                        };

                        await channel.BasicConsumeAsync(QueueName, autoAck: false, consumer: consumer, stoppingToken);
                        logger.LogInformation("[DOCKER-HOST] Listening on queue '{Queue}'", QueueName);

                        await Task.Delay(Timeout.Infinite, stoppingToken);
                    }
                    catch (OperationCanceledException) { return; }
                    catch (Exception ex) when (DockerInventoryConsumer.IsTopologyNotReady(ex))
                    {
                        logger.LogWarning(
                            "[STARTUP-WAIT] Queue {Queue} not ready — waiting for InfinityAI.Api topology.", QueueName);
                        try { await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken); }
                        catch (OperationCanceledException) { return; }
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning("[DOCKER-HOST] Channel error ({Msg}) — reconnecting", ex.Message);
                        break;
                    }
                    finally
                    {
                        if (channel is not null)
                            try { await channel.DisposeAsync(); } catch { }
                    }
                }
            }
        }

        logger.LogInformation("[DOCKER-HOST] DockerHostMetricsConsumer stopped.");
    }

    private async Task<bool> HandleReceivedAsync(byte[] body, CancellationToken ct)
    {
        DockerHostMetricsMessage? msg;
        try
        {
            msg = JsonSerializer.Deserialize<DockerHostMetricsMessage>(
                Encoding.UTF8.GetString(body), _json);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[DOCKER-HOST] Failed to deserialize DockerHostMetricsMessage — discarding");
            return false;
        }

        if (msg is null)
        {
            logger.LogWarning("[DOCKER-HOST] Deserialized null message — discarding");
            return false;
        }

        try
        {
            await hubContext.Clients
                .Group(DockerSignalRSchema.DockerHostGroup)
                .SendAsync(DockerSignalRSchema.EventHostMetrics, msg, ct);

            logger.LogDebug(
                "[DOCKER-HOST] Forwarded {Event}: Score={Score} Cpu={Cpu:F1}% Mem={Mem:F1}% Disk={Disk:F1}%",
                DockerSignalRSchema.EventHostMetrics,
                msg.HostHealthScore, msg.Cpu.UsagePercent, msg.Memory.UsagePercent, msg.Disk.UsagePercent);

            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[DOCKER-HOST] Failed to forward {Event} to SignalR group",
                DockerSignalRSchema.EventHostMetrics);
            return false;
        }
    }
}
