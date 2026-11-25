using Azure.Messaging.ServiceBus;

namespace ServiceBusDemo.Producer;

internal static class DeadLetterEndpoints
{
    public static void MapDeadLetters(this WebApplication app, string businessTopic, string broadcastTopic, string routedTopic)
    {
        var configuration = app.Configuration;
        (DeadLetterSubscription Pair, string SenderKey)[] topology =
        [
            (Pair(businessTopic, "SubscriptionName", "business-events"), SenderKeys.BusinessEvents),
            (Pair(broadcastTopic, "ConsumerOneBroadcastSubscriptionName", "consumer-one"), SenderKeys.BroadcastEvents),
            (Pair(broadcastTopic, "ConsumerTwoBroadcastSubscriptionName", "consumer-two"), SenderKeys.BroadcastEvents),
            (Pair(routedTopic, "HighPrioritySubscriptionName", "high-priority"), SenderKeys.RoutedEvents),
            (Pair(routedTopic, "EuOrdersSubscriptionName", "eu-orders"), SenderKeys.RoutedEvents),
            (Pair(routedTopic, "AllRoutedSubscriptionName", "all"), SenderKeys.RoutedEvents)
        ];

        var endpoints = app.MapGroup("/dead-letters");
        endpoints.MapGet("/subscriptions", () => topology.Select(entry => entry.Pair).ToArray());
        endpoints.MapGet("/messages", async (string topic, string subscription, long? fromSequenceNumber,
            DeadLetterService service, CancellationToken token) =>
        {
            var pair = new DeadLetterSubscription(topic, subscription);
            if (!topology.Any(entry => entry.Pair == pair)) return UnknownPair();
            return await service.PeekAsync(pair, fromSequenceNumber, token);
        });
        endpoints.MapPost("/messages/{sequenceNumber:long}/resubmit", (string topic, string subscription,
            long sequenceNumber, HttpContext context, DeadLetterService service, CancellationToken token) =>
        {
            var pair = new DeadLetterSubscription(topic, subscription);
            var key = topology.FirstOrDefault(entry => entry.Pair == pair).SenderKey;
            return key is null ? Task.FromResult(UnknownPair()) : service.ActAsync(pair, sequenceNumber,
                context.RequestServices.GetRequiredKeyedService<ServiceBusSender>((DeadLetterService.ClientKey, key)), token);
        });
        endpoints.MapPost("/messages/{sequenceNumber:long}/purge", (string topic, string subscription,
            long sequenceNumber, DeadLetterService service, CancellationToken token) =>
        {
            var pair = new DeadLetterSubscription(topic, subscription);
            return !topology.Any(entry => entry.Pair == pair) ? Task.FromResult(UnknownPair()) :
                service.ActAsync(pair, sequenceNumber, sender: null, token);
        });

        DeadLetterSubscription Pair(string topic, string setting, string fallback) =>
            new(topic, configuration[$"ServiceBus:{setting}"] ?? fallback);
        static IResult UnknownPair() => Results.Text("Unknown dead-letter topic/subscription pair.", statusCode: 400);
    }
}
