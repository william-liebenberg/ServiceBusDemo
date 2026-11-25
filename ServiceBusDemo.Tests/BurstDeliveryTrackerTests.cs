using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;

namespace ServiceBusDemo.Tests;

public class BurstDeliveryTrackerTests
{
    [Fact]
    public async Task ReconcilesEarlyAndConcurrentReportsWithoutDependingOnCards()
    {
        var feed = new DeliveryFeed();
        using var tracker = new BurstDeliveryTracker(feed, MessagingPattern.PubSub, 250);
        var token = TestContext.Current.CancellationToken;

        for (var i = 0; i < 250; i++)
        {
            tracker.BeginRequest();
            feed.Add(Report(i.ToString()));
            feed.Add(Report("unrelated"));
            Assert.Equal(i, tracker.Snapshot().ConsumerOne);
            tracker.EndRequest(i.ToString());
        }

        Assert.Equal(200, feed.Snapshot().Count);
        feed.Clear();
        Assert.Equal((250, 250, 0, false), tracker.Snapshot());
        await Parallel.ForEachAsync(Enumerable.Range(0, 250), token, (i, _) =>
        {
            var id = i.ToString();
            feed.Add(Report(id));
            feed.Add(Report(id, "ConsumerTwo", "CompetingConsumers"));
            feed.Add(Report(id, "ConsumerThree"));
            feed.Add(Report(id, "ConsumerTwo"));
            feed.Add(Report(id, "ConsumerTwo"));
            return ValueTask.CompletedTask;
        });
        feed.Add(Report("unrelated", "ConsumerTwo"));
        Assert.Equal((250, 250, 250, false), tracker.Snapshot());
    }

    [Fact]
    public void BoundsEarlyIdsAndClearsThemAfterEachRequest()
    {
        var feed = new DeliveryFeed();
        using var tracker = new BurstDeliveryTracker(feed, MessagingPattern.PubSub, 2);
        tracker.BeginRequest();
        for (var i = 0; i < BurstDeliveryTracker.EarlyIdCapacity; i++)
        {
            feed.Add(Report(i.ToString()));
            feed.Add(Report(i.ToString()));
        }
        Assert.False(tracker.Snapshot().Overflowed);
        feed.Add(Report("overflow"));
        feed.Add(Report("0", "ConsumerTwo"));
        tracker.EndRequest("0");
        tracker.BeginRequest();
        tracker.EndRequest("1");
        Assert.Equal((2, 1, 1, true), tracker.Snapshot());
    }

    [Fact]
    public void DisposedTrackerIgnoresNotificationsAndIsUnsubscribed()
    {
        var feed = new DeliveryFeed();
        using var tracker = new BurstDeliveryTracker(feed, MessagingPattern.PubSub, 1);
        tracker.BeginRequest();
        tracker.EndRequest("sent");
        Assert.Same(tracker, Assert.Single(Subscribers(feed)).Target);
        tracker.Dispose();
        Assert.Empty(Subscribers(feed));
        feed.Add(Report("sent"));
        Assert.Equal((1, 0, 0, false), tracker.Snapshot());
    }

    internal static Delegate[] Subscribers(DeliveryFeed feed) =>
        (typeof(DeliveryFeed).GetField("Reported", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(feed) as Delegate)?.GetInvocationList() ?? [];

    private static DeliveryEvent Report(string id, string consumer = "ConsumerOne", string pattern = "PubSub") =>
        new(id, pattern, consumer, "broadcast-events", "consumer-one", "Order placed", 1, DateTimeOffset.UnixEpoch,
            DeliveryOutcomes.Completed, DateTimeOffset.UnixEpoch.AddMinutes(1), DateTimeOffset.UnixEpoch);
}
