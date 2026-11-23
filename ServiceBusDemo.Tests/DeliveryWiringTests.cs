namespace ServiceBusDemo.Tests;

public class DeliveryWiringTests
{
    [Fact]
    public async Task ConsumersReferenceWebWithoutChangingStartupWaits()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var builder = await DistributedApplicationTestingBuilder.CreateAsync<Projects.ServiceBusDemo_AppHost>(cancellationToken);
        await using var app = await builder.BuildAsync(cancellationToken);
        var resources = app.Services.GetRequiredService<DistributedApplicationModel>().Resources;
        var web = resources.Single(resource => resource.Name == "webfrontend");
        var consumerOne = resources.Single(resource => resource.Name == "consumer-one");
        var consumerTwo = resources.Single(resource => resource.Name == "consumer-two");

        AssertWebReference(consumerOne, web);
        AssertWebReference(consumerTwo, web);
        Assert.Equal("producer", Assert.Single(WaitTargets(web)).Name);
        AssertConsumerWaits(consumerOne, "broadcast-consumer-one");
        AssertConsumerWaits(consumerTwo, "broadcast-consumer-two");

        foreach (var resource in resources)
        {
            AssertAcyclicWaits(resource, new HashSet<IResource>(ReferenceEqualityComparer.Instance));
        }
    }

    private static void AssertWebReference(IResource consumer, IResource web) =>
        Assert.Contains(consumer.Annotations.OfType<ResourceRelationshipAnnotation>(),
            annotation => annotation.Type == "Reference" && ReferenceEquals(annotation.Resource, web));

    private static void AssertConsumerWaits(IResource consumer, string broadcastSubscription)
    {
        var waits = WaitTargets(consumer).Select(resource => resource.Name).ToArray();
        Assert.Contains("business-events", waits);
        Assert.Contains(broadcastSubscription, waits);
        Assert.DoesNotContain("webfrontend", waits);
    }

    private static IEnumerable<IResource> WaitTargets(IResource resource) =>
        resource.Annotations.OfType<WaitAnnotation>().Select(wait => wait.Resource);

    private static void AssertAcyclicWaits(IResource resource, HashSet<IResource> path)
    {
        Assert.True(path.Add(resource), $"Circular startup wait involving {resource.Name}.");
        foreach (var dependency in WaitTargets(resource))
        {
            AssertAcyclicWaits(dependency, path);
        }

        path.Remove(resource);
    }
}
