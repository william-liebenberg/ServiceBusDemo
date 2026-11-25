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
    public void ClearNotifiesAfterEmptyingFeedAndPreservesEarlierSnapshots()
    {
        var feed = new DeliveryFeed();
        var first = CreateDelivery(1);
        feed.Add(first);
        var beforeClear = feed.Snapshot();
        var notifications = 0;
        feed.Changed += VerifyCleared;

        feed.Clear();
        feed.Clear();
        feed.Changed -= VerifyCleared;
        var afterClear = feed.Snapshot();
        var later = CreateDelivery(2);
        feed.Add(later);

        Assert.Equal(2, notifications);
        Assert.Equal(first, Assert.Single(beforeClear));
        Assert.Empty(afterClear);
        Assert.Equal(later, Assert.Single(feed.Snapshot()));

        void VerifyCleared()
        {
            notifications++;
            Assert.Empty(feed.Snapshot());
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConcurrentUpdatesAndSnapshotsRespectCapacity(bool clearConcurrently)
    {
        const int workerCount = 8;
        const int reportsPerWorker = 100;
        var token = TestContext.Current.CancellationToken;
        var feed = new DeliveryFeed();
        var notifications = 0;
        feed.Changed += () => Interlocked.Increment(ref notifications);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var workers = Enumerable.Range(0, workerCount).Select(worker => Task.Run(async () =>
        {
            await start.Task;
            for (var index = 0; index < reportsPerWorker; index++)
            {
                feed.Add(CreateDelivery(worker * reportsPerWorker + index));
                var snapshot = feed.Snapshot();
                Assert.InRange(snapshot.Count, clearConcurrently ? 0 : 1, 200);
                Assert.Equal(snapshot.Count, snapshot.Distinct().Count());
            }
        }, token)).ToList();

        if (clearConcurrently)
        {
            workers.Add(Task.Run(async () =>
            {
                await start.Task;
                for (var index = 0; index < reportsPerWorker; index++)
                {
                    feed.Clear();
                    await Task.Yield();
                }
            }, token));
        }

        start.SetResult();
        await Task.WhenAll(workers);

        Assert.Equal((workerCount + (clearConcurrently ? 1 : 0)) * reportsPerWorker, notifications);
        if (clearConcurrently)
        {
            feed.Clear();
            var later = CreateDelivery(999);
            feed.Add(later);
            Assert.Equal(later, Assert.Single(feed.Snapshot()));
        }
        else
        {
            Assert.Equal(200, feed.Snapshot().Count);
        }
    }

    private static DeliveryEvent CreateDelivery(int id) => new(
        id.ToString(), "CompetingConsumers", "ConsumerOne", "business-events", "business-events", "Order placed", 1,
        DateTimeOffset.UnixEpoch, DeliveryOutcomes.Completed, DateTimeOffset.UnixEpoch.AddMinutes(1),
        DateTimeOffset.UnixEpoch);
}
