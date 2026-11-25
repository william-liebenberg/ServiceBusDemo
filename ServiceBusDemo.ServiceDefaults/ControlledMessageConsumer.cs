using System.Text;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ServiceBusDemo.ServiceDefaults;

public abstract class ControlledMessageConsumer : BackgroundService
{
    private readonly ServiceBusClient _client;
    private readonly ILogger _logger;
    private readonly DeliveryReportQueue _reports;
    private readonly ProcessorSlot[] _processors;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private volatile ConsumerStatus _status;
    private bool _initialized;
    private volatile bool _stopping;

    protected ControlledMessageConsumer(ServiceBusClient client, IConfiguration configuration, ILogger logger,
        DeliveryReportQueue reports, string consumerName, string broadcastSubscription, params string[] routedSubscriptions)
    {
        _client = client;
        _logger = logger;
        _reports = reports;
        var routedTopic = configuration["ServiceBus:RoutedTopicName"] ?? "routed-events";
        _processors =
        [
            new("CompetingConsumers", configuration["ServiceBus:TopicName"] ?? "business-events",
                configuration["ServiceBus:SubscriptionName"] ?? "business-events"),
            new("PubSub", configuration["ServiceBus:BroadcastTopicName"] ?? "broadcast-events",
                configuration["ServiceBus:BroadcastSubscriptionName"] ?? broadcastSubscription),
            .. routedSubscriptions.Select(subscription => new ProcessorSlot("TopicFilters", routedTopic, subscription))
        ];
        _status = new(consumerName, ConsumerState.Initializing, SnapshotSubscriptions());
    }

    public ConsumerStatus GetStatus()
    {
        var snapshot = _status;
        return _stopping ? snapshot with { State = ConsumerState.Unavailable } : snapshot;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _gate.WaitAsync(stoppingToken);
        try
        {
            foreach (var slot in _processors)
            {
                if (_stopping)
                {
                    return;
                }

                var processor = _client.CreateProcessor(slot.Topic, slot.Subscription, new ServiceBusProcessorOptions
                {
                    AutoCompleteMessages = true,
                    MaxConcurrentCalls = 1
                });
                slot.Processor = processor;
                processor.ProcessMessageAsync += args => ReportDeliveryAsync(slot, args);
                processor.ProcessErrorAsync += args =>
                {
                    _logger.LogError(args.Exception, "[{Pattern}] {Consumer} error processing message from {EntityPath}",
                        slot.Pattern, _status.Consumer, args.EntityPath);
                    return Task.CompletedTask;
                };

                await processor.StartProcessingAsync(stoppingToken);
                slot.State = ConsumerState.Running;
                PublishStatus(ConsumerState.Initializing);
                _logger.LogInformation("[{Pattern}] {Consumer} listening on {Topic}/{Subscription}",
                    slot.Pattern, _status.Consumer, slot.Topic, slot.Subscription);
            }
            _initialized = true;
            PublishStatus(ConsumerState.Running);
        }
        catch
        {
            PublishStatus(ConsumerState.Unavailable);
            throw;
        }
        finally
        {
            _gate.Release();
        }
    }

    public Task<(bool Succeeded, ConsumerStatus Status)> PauseAsync(CancellationToken cancellationToken) =>
        ChangeStateAsync(ConsumerState.Paused, cancellationToken);

    public Task<(bool Succeeded, ConsumerStatus Status)> ResumeAsync(CancellationToken cancellationToken) =>
        ChangeStateAsync(ConsumerState.Running, cancellationToken);

    private async Task<(bool Succeeded, ConsumerStatus Status)> ChangeStateAsync(ConsumerState target, CancellationToken cancellationToken)
    {
        if (_stopping || _status.State == ConsumerState.Initializing)
        {
            return (false, GetStatus());
        }

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_stopping || !_initialized)
            {
                return (false, GetStatus());
            }
            if (_status.State == target)
            {
                return (true, GetStatus());
            }

            var succeeded = true;
            PublishStatus(ConsumerState.Transitioning);
            foreach (var slot in _processors)
            {
                if (_stopping)
                {
                    break;
                }
                if (slot.State == target)
                {
                    continue;
                }

                var processor = slot.Processor!;
                try
                {
                    // Once admitted, finish the SDK operation even if the caller disconnects.
                    if (target == ConsumerState.Paused)
                    {
                        await processor.StopProcessingAsync();
                    }
                    else
                    {
                        await processor.StartProcessingAsync();
                    }
                    slot.State = target;
                }
                catch (Exception ex)
                {
                    succeeded = false;
                    slot.State = processor.IsProcessing ? ConsumerState.Running : ConsumerState.Paused;
                    _logger.LogError(ex, "Could not set {Consumer} processor {Topic}/{Subscription} to {State}",
                        _status.Consumer, slot.Topic, slot.Subscription, target);
                }
                PublishStatus(ConsumerState.Transitioning);
            }

            var actual = _processors[0].State;
            var aggregate = _processors.All(slot => slot.State == actual) ? actual : ConsumerState.Mixed;
            PublishStatus(!succeeded && aggregate == target ? ConsumerState.Unavailable : aggregate);
            var status = GetStatus();
            return (succeeded && status.State == target, status);
        }
        finally
        {
            _gate.Release();
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _stopping = true;
        await base.StopAsync(cancellationToken);
        // Do not dispose while a start/stop is still running, even if the host's deadline has elapsed.
        await _gate.WaitAsync();
        try
        {
            foreach (var slot in _processors)
            {
                if (slot.Processor is { } processor)
                {
                    try
                    {
                        // Close waits for handlers and closes the underlying receivers.
                        await processor.DisposeAsync();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Could not close {Consumer} processor {Topic}/{Subscription}",
                            _status.Consumer, slot.Topic, slot.Subscription);
                    }
                    slot.Processor = null;
                }
                slot.State = ConsumerState.Unavailable;
                PublishStatus(ConsumerState.Unavailable);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    private void PublishStatus(ConsumerState state) => _status = new(_status.Consumer, state, SnapshotSubscriptions());

    private IReadOnlyList<SubscriptionStatus> SnapshotSubscriptions() => Array.AsReadOnly(_processors
        .Select(slot => new SubscriptionStatus(slot.Pattern, slot.Topic, slot.Subscription, slot.State)).ToArray());

    private Task ReportDeliveryAsync(ProcessorSlot slot, ProcessMessageEventArgs args)
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var body = Encoding.UTF8.GetString(args.Message.Body.ToArray());
        var consumer = _status.Consumer;
        _logger.LogInformation(
            "[{Pattern}] {Consumer} received message {MessageId} from {Topic}/{Subscription}: {Body}",
            slot.Pattern, consumer, args.Message.MessageId, slot.Topic, slot.Subscription, body);
        _reports.Enqueue(new DeliveryEvent(args.Message.MessageId, slot.Pattern, consumer, slot.Topic,
            slot.Subscription, body, args.Message.DeliveryCount, receivedAt));
        return Task.CompletedTask;
    }

    private sealed class ProcessorSlot(string pattern, string topic, string subscription)
    {
        public string Pattern { get; } = pattern;
        public string Topic { get; } = topic;
        public string Subscription { get; } = subscription;
        public ConsumerState State { get; set; } = ConsumerState.Initializing;
        public ServiceBusProcessor? Processor { get; set; }
    }
}
