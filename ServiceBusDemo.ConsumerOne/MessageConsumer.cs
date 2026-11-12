using System.Text;
using Azure.Messaging.ServiceBus;

namespace ServiceBusDemo.ConsumerOne;

public sealed class MessageConsumer(
    ServiceBusClient client,
    IConfiguration configuration,
    ILogger<MessageConsumer> logger) : BackgroundService
{
    private const string ConsumerName = "ConsumerOne";
    private readonly List<ServiceBusProcessor> _processors = [];

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        (string Pattern, string Topic, string Subscription)[] subscriptions =
        [
            ("CompetingConsumers",
                configuration["ServiceBus:TopicName"] ?? "business-events",
                configuration["ServiceBus:SubscriptionName"] ?? "business-events"),
            ("PubSub",
                configuration["ServiceBus:BroadcastTopicName"] ?? "broadcast-events",
                configuration["ServiceBus:BroadcastSubscriptionName"] ?? "consumer-one"),
        ];

        foreach (var (pattern, topic, subscription) in subscriptions)
        {
            var processor = client.CreateProcessor(topic, subscription, new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = true,
                MaxConcurrentCalls = 1
            });

            processor.ProcessMessageAsync += args =>
            {
                logger.LogInformation(
                    "[{Pattern}] {Consumer} received message {MessageId} from {Topic}/{Subscription}: {Body}",
                    pattern,
                    ConsumerName,
                    args.Message.MessageId,
                    topic,
                    subscription,
                    Encoding.UTF8.GetString(args.Message.Body.ToArray()));
                return Task.CompletedTask;
            };

            processor.ProcessErrorAsync += args =>
            {
                logger.LogError(args.Exception, "[{Pattern}] {Consumer} error processing message from {EntityPath}",
                    pattern, ConsumerName, args.EntityPath);
                return Task.CompletedTask;
            };

            _processors.Add(processor);
            await processor.StartProcessingAsync(stoppingToken);
            logger.LogInformation("[{Pattern}] {Consumer} listening on {Topic}/{Subscription}", pattern, ConsumerName, topic, subscription);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var processor in _processors)
        {
            await processor.StopProcessingAsync(cancellationToken);
            await processor.DisposeAsync();
        }

        await base.StopAsync(cancellationToken);
    }
}
