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

    private readonly int[] pendingEstimates = new int[depthSubscriptions.Length];
    private readonly int[] announcedEstimates = new int[depthSubscriptions.Length];
    private bool depthAnnouncementPending;
    private string? depthAnnouncement;

    private void QueueDepthAnnouncement()
    {
        if (disposed || depthAnnouncementPending || pendingEstimates.SequenceEqual(announcedEstimates))
        {
            return;
        }

        depthAnnouncementPending = true;
        _ = AnnounceDepthAsync(pageLifetime.Token);
    }

    private async Task AnnounceDepthAsync(CancellationToken cancellationToken)
    {
        try
        {
            // Use a fixed window so a sustained burst cannot postpone the announcement indefinitely.
            await Task.Delay(TimeSpan.FromMilliseconds(750), cancellationToken);
            await InvokeAsync(() =>
            {
                depthAnnouncementPending = false;
                if (disposed)
                {
                    return;
                }

                var changes = new List<string>();
                for (var index = 0; index < depthSubscriptions.Length; index++)
                {
                    var count = pendingEstimates[index];
                    if (count == announcedEstimates[index])
                    {
                        continue;
                    }

                    var (topic, subscription) = depthSubscriptions[index];
                    changes.Add($"Topic {topic}, subscription {subscription}: estimated pending {count}.");
                    announcedEstimates[index] = count;
                }

                if (changes.Count > 0)
                {
                    depthAnnouncement = string.Join(" ", changes);
                    StateHasChanged();
                }
            });
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The page was disposed before the announcement window ended.
        }
    }

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
