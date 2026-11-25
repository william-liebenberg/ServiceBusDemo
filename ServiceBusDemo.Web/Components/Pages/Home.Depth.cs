using ServiceBusDemo.ServiceDefaults;

namespace ServiceBusDemo.Web.Components.Pages;

public partial class Home
{
    private static readonly (string Topic, string Subscription)[] depthSubscriptions =
    [
        ("business-events", "business-events"),
        ("broadcast-events", "consumer-one"),
        ("broadcast-events", "consumer-two")
    ];

    private int EstimatePending(string topic, string subscription, IReadOnlyList<DeliveryEvent> snapshot)
    {
        var reportedIds = snapshot.Where(report => report.Topic == topic && report.Subscription == subscription)
            .Select(report => report.MessageId).ToHashSet(StringComparer.Ordinal);

        // The set difference counts distinct acknowledged IDs, never going below zero.
        return sentMessages.Where(sent => sent.Topic == topic)
            .Select(sent => sent.MessageId)
            .Except(reportedIds, StringComparer.Ordinal)
            .Count();
    }
}
