using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ServiceBusDemo.ServiceDefaults;

public sealed class DeliveryReportQueue(ILogger<DeliveryReportQueue> logger)
{
    private readonly Channel<DeliveryEvent> _channel = Channel.CreateBounded<DeliveryEvent>(new BoundedChannelOptions(256)
    {
        SingleReader = true,
        FullMode = BoundedChannelFullMode.Wait,
        AllowSynchronousContinuations = false
    });

    public void Enqueue(DeliveryEvent delivery)
    {
        // TryWrite never waits; Wait mode lets us detect a full queue instead of silently losing reports.
        if (!_channel.Writer.TryWrite(delivery))
        {
            logger.LogWarning("Dropping delivery report for {MessageId}: reporting queue is full or closed", delivery.MessageId);
        }
    }

    internal ChannelReader<DeliveryEvent> Reader => _channel.Reader;

    internal void DiscardPending()
    {
        _channel.Writer.TryComplete();
        while (_channel.Reader.TryRead(out _)) { }
    }
}

internal sealed class DeliveryReportWorker(
    DeliveryReportQueue queue,
    DeliveryReporter reporter,
    IHostApplicationLifetime lifetime,
    ILogger<DeliveryReportWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var shutdown = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, lifetime.ApplicationStopping);
        var token = shutdown.Token;
        try
        {
            await foreach (var delivery in queue.Reader.ReadAllAsync(token))
            {
                token.ThrowIfCancellationRequested();
                try
                {
                    using var response = await reporter.PostAsync(delivery, token);
                    if (!response.IsSuccessStatusCode)
                    {
                        logger.LogWarning("Delivery report for {MessageId} returned HTTP {StatusCode}",
                            delivery.MessageId, (int)response.StatusCode);
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
                catch (OperationCanceledException ex)
                {
                    logger.LogWarning(ex, "Delivery report for {MessageId} timed out", delivery.MessageId);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Delivery report for {MessageId} failed", delivery.MessageId);
                }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Normal shutdown while waiting for a report.
        }
        finally
        {
            queue.DiscardPending();
        }
    }
}
