namespace ServiceBusDemo.Tests;

public class DeliveryWiringTests
{
    [Fact]
    public async Task ConsumersAndWebReferenceEachOtherWithoutChangingStartupWaits()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var builder = await DistributedApplicationTestingBuilder.CreateAsync<Projects.ServiceBusDemo_AppHost>(cancellationToken);
        await using var app = await builder.BuildAsync(cancellationToken);
        var resources = app.Services.GetRequiredService<DistributedApplicationModel>().Resources;
        var web = resources.Single(resource => resource.Name == "webfrontend");
        var consumerOne = resources.Single(resource => resource.Name == "consumer-one");
        var consumerTwo = resources.Single(resource => resource.Name == "consumer-two");

        foreach (var consumer in new[] { consumerOne, consumerTwo })
        {
            AssertReference(consumer, web);
            AssertReference(web, consumer);
            var endpoints = consumer.Annotations.OfType<EndpointAnnotation>().ToArray();
            Assert.NotEmpty(endpoints);
            Assert.All(endpoints, endpoint => Assert.False(endpoint.IsExternal));
        }
        Assert.Equal("producer", Assert.Single(WaitTargets(web)).Name);
        AssertConsumerWaits(consumerOne, "broadcast-consumer-one");
        AssertConsumerWaits(consumerTwo, "broadcast-consumer-two");

        foreach (var resource in resources)
        {
            AssertAcyclicWaits(resource, new HashSet<IResource>(ReferenceEqualityComparer.Instance));
        }
    }

    private static void AssertReference(IResource from, IResource to) =>
        Assert.Contains(from.Annotations.OfType<ResourceRelationshipAnnotation>(),
            annotation => annotation.Type == "Reference" && ReferenceEquals(annotation.Resource, to));

    private static void AssertConsumerWaits(IResource consumer, string broadcastSubscription)
    {
        var waits = WaitTargets(consumer).ToArray();
        Assert.Contains(waits, resource => resource.Name == broadcastSubscription);
        Assert.Contains(waits, resource => resource.Name == "business-events");
        // Aspire can also add implicit waits on broker resources, but not on application projects.
        Assert.DoesNotContain(waits, resource => resource is ProjectResource);
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
