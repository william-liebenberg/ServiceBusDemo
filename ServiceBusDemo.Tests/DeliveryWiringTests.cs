using Aspire.Hosting.Azure;

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
        AssertConsumerWaits(consumerOne, "broadcast-consumer-one", "high-priority", "routed-all");
        AssertConsumerWaits(consumerTwo, "broadcast-consumer-two", "eu-orders");
        var producer = resources.Single(resource => resource.Name == "producer");
        var producerWaits = WaitTargets(producer).ToArray();
        AssertRule("high-priority", "Priority", "High");
        AssertRule("eu-orders", "Region", "EU");
        var all = Assert.IsType<AzureServiceBusSubscriptionResource>(resources.Single(resource => resource.Name == "routed-all"));
        Assert.Equal("all", all.SubscriptionName);
        Assert.Empty(all.Rules);
        Assert.Contains(all, producerWaits);

        void AssertRule(string name, string property, string value)
        {
            var subscription = Assert.IsType<AzureServiceBusSubscriptionResource>(resources.Single(resource => resource.Name == name));
            Assert.Equal(name, subscription.SubscriptionName);
            Assert.Contains(subscription, producerWaits);
            var rule = Assert.Single(subscription.Rules);
            Assert.Equal(AzureServiceBusFilterType.CorrelationFilter, rule.FilterType);
            Assert.Equal(value, Assert.Single(rule.CorrelationFilter.Properties, entry => entry.Key == property).Value);
        }

        foreach (var resource in resources)
        {
            AssertAcyclicWaits(resource, new HashSet<IResource>(ReferenceEqualityComparer.Instance));
        }
    }

    private static void AssertReference(IResource from, IResource to) =>
        Assert.Contains(from.Annotations.OfType<ResourceRelationshipAnnotation>(),
            annotation => annotation.Type == "Reference" && ReferenceEquals(annotation.Resource, to));

    private static void AssertConsumerWaits(IResource consumer, string broadcastSubscription, params string[] routed)
    {
        var waits = WaitTargets(consumer).ToArray();
        Assert.Contains(waits, resource => resource.Name == broadcastSubscription);
        Assert.Contains(waits, resource => resource.Name == "business-events");
        Assert.Equal(routed.Order(StringComparer.Ordinal), waits.Select(resource => resource.Name)
            .Where(name => name is "high-priority" or "eu-orders" or "routed-all").Order(StringComparer.Ordinal));

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
