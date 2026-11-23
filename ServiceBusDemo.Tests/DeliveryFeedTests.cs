using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;

namespace ServiceBusDemo.Tests;

public class DeliveryFeedTests
{
    [Fact]
    public void AddStoresDeliveryBeforeNotifying()
    {
        var feed = new DeliveryFeed();
        var delivery = CreateDelivery(1);
        var notifications = 0;
        feed.Changed += () =>
        {
            notifications++;
            Assert.Equal(delivery, Assert.Single(feed.Snapshot()));
        };

        Assert.Empty(feed.Snapshot());
        feed.Add(delivery);

        Assert.Equal(1, notifications);
    }

    [Fact]
    public void SnapshotIsIsolatedFromLaterAdditionsAndCallerEdits()
    {
        var feed = new DeliveryFeed();
        var first = CreateDelivery(1);
        var second = CreateDelivery(2);
        feed.Add(first);
        var snapshot = feed.Snapshot();

        feed.Add(second);
        Assert.Equal(first, Assert.Single(snapshot));

        if (snapshot is IList<DeliveryEvent> mutable && snapshot is DeliveryEvent[])
        {
            mutable[0] = second;
        }

        Assert.Equal(new[] { first, second }, feed.Snapshot());
    }

    [Fact]
    public void AddRetainsLatest200InInsertionOrderAndNotifiesForEveryAddition()
    {
        var feed = new DeliveryFeed();
        var notifications = 0;
        feed.Changed += () => notifications++;
        var deliveries = Enumerable.Range(0, 250).Select(CreateDelivery).ToArray();

        foreach (var delivery in deliveries)
        {
            feed.Add(delivery);
        }

        Assert.Equal(deliveries.Skip(50), feed.Snapshot());
        Assert.Equal(deliveries.Length, notifications);
    }

    [Fact]
    public async Task ConcurrentAddsAndSnapshotsRespectCapacity()
    {
        var feed = new DeliveryFeed();
        var notifications = 0;
        feed.Changed += () => Interlocked.Increment(ref notifications);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var workers = Enumerable.Range(0, 8).Select(worker => Task.Run(async () =>
        {
            await start.Task;
            for (var index = 0; index < 100; index++)
            {
                feed.Add(CreateDelivery(worker * 100 + index));
                var snapshot = feed.Snapshot();
                Assert.InRange(snapshot.Count, 1, 200);
                Assert.Equal(snapshot.Count, snapshot.Distinct().Count());
            }
        })).ToArray();

        start.SetResult();
        await Task.WhenAll(workers);

        Assert.Equal(200, feed.Snapshot().Count);
        Assert.Equal(800, notifications);
    }

    private static DeliveryEvent CreateDelivery(int id) => new(
        id.ToString(), "CompetingConsumers", "consumer-one", "business-events", "business-events", "Order placed", 1,
        DateTimeOffset.UnixEpoch);
}
