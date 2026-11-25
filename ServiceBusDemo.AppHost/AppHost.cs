using System.Globalization;
using System.Text.Json.Nodes;
using Aspire.Hosting.Azure;

var builder = DistributedApplication.CreateBuilder(args);
const int emulatorMaxDeliveryCount = 5;

var asb = builder.AddAzureServiceBus("messaging")
    .RunAsEmulator(emulator => emulator.WithConfiguration(configuration =>
    {
        var namespaceConfiguration = configuration["UserConfig"]?["Namespaces"]?[0]
            ?? throw new InvalidOperationException("The Azure Service Bus emulator configuration must define a namespace.");
        namespaceConfiguration["Name"] = "sbemulatorns";
        foreach (var topic in namespaceConfiguration["Topics"]!.AsArray())
        {
            foreach (var subscription in topic!["Subscriptions"]!.AsArray())
            {
                if (subscription!["Properties"] is not JsonObject properties)
                {
                    subscription["Properties"] = properties = new JsonObject();
                }
                properties["MaxDeliveryCount"] = emulatorMaxDeliveryCount;
            }
        }
    }));

// Competing consumers: both consumers share a single subscription, so each message is processed by only one of them.
var beTopic = asb.AddServiceBusTopic("businessEvent", topicName: "business-events");
var beSub = beTopic.AddServiceBusSubscription("business-events");

// Publish/subscribe: each consumer has its own subscription, so every consumer receives a copy of each message.
var broadcastTopic = asb.AddServiceBusTopic("broadcastEvent", topicName: "broadcast-events");
var consumerOneBroadcastSub = broadcastTopic.AddServiceBusSubscription("broadcast-consumer-one", subscriptionName: "consumer-one");
var consumerTwoBroadcastSub = broadcastTopic.AddServiceBusSubscription("broadcast-consumer-two", subscriptionName: "consumer-two");

// Topic filters: Aspire emits these correlation rules in the emulator's startup configuration.
var routedTopic = asb.AddServiceBusTopic("routed-events");
var highPriority = routedTopic.AddServiceBusSubscription("high-priority");
var euOrders = routedTopic.AddServiceBusSubscription("eu-orders");
var allRouted = routedTopic.AddServiceBusSubscription("routed-all", subscriptionName: "all");
highPriority.Resource.Rules.Add(MatchProperty("priority-is-high", "Priority", "High"));
euOrders.Resource.Rules.Add(MatchProperty("region-is-eu", "Region", "EU"));

var producer = builder.AddProject<Projects.ServiceBusDemo_Producer>("producer")
    .WithHttpHealthCheck("/health")
    .WithReference(asb)
    .WithEnvironment("ServiceBus__TopicName", beTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__BroadcastTopicName", broadcastTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__RoutedTopicName", routedTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__SubscriptionName", beSub.Resource.SubscriptionName)
    .WithEnvironment("ServiceBus__ConsumerOneBroadcastSubscriptionName", consumerOneBroadcastSub.Resource.SubscriptionName)
    .WithEnvironment("ServiceBus__ConsumerTwoBroadcastSubscriptionName", consumerTwoBroadcastSub.Resource.SubscriptionName)
    .WithEnvironment("ServiceBus__HighPrioritySubscriptionName", highPriority.Resource.SubscriptionName)
    .WithEnvironment("ServiceBus__EuOrdersSubscriptionName", euOrders.Resource.SubscriptionName)
    .WithEnvironment("ServiceBus__AllRoutedSubscriptionName", allRouted.Resource.SubscriptionName)
    .WaitFor(beTopic)
    .WaitFor(broadcastTopic)
    .WaitFor(highPriority)
    .WaitFor(euOrders)
    .WaitFor(allRouted);

// The web frontend is the producer API's UI; parenting nests it under the producer in the dashboard.
var webfrontend = builder.AddProject<Projects.ServiceBusDemo_Web>("webfrontend")
    .WithExternalHttpEndpoints()
    .WithHttpHealthCheck("/health")
    .WithEnvironment("ServiceBus__EmulatorMaxDeliveryCount", emulatorMaxDeliveryCount.ToString(CultureInfo.InvariantCulture))
    .WithReference(producer)
    .WithParentRelationship(producer)
    .WaitFor(producer);

var consumerOne = builder.AddProject<Projects.ServiceBusDemo_ConsumerOne>("consumer-one")
    .WithHttpHealthCheck("/health")
    .WithReference(asb)
    .WithReference(webfrontend)
    .WithEnvironment("ServiceBus__TopicName", beTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__SubscriptionName", beSub.Resource.SubscriptionName)
    .WithEnvironment("ServiceBus__BroadcastTopicName", broadcastTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__BroadcastSubscriptionName", consumerOneBroadcastSub.Resource.SubscriptionName)
    .WithEnvironment("ServiceBus__RoutedTopicName", routedTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__HighPrioritySubscriptionName", highPriority.Resource.SubscriptionName)
    .WithEnvironment("ServiceBus__AllRoutedSubscriptionName", allRouted.Resource.SubscriptionName)
    .WaitFor(beSub)
    .WaitFor(consumerOneBroadcastSub)
    .WaitFor(highPriority)
    .WaitFor(allRouted);

var consumerTwo = builder.AddProject<Projects.ServiceBusDemo_ConsumerTwo>("consumer-two")
    .WithHttpHealthCheck("/health")
    .WithReference(asb)
    .WithReference(webfrontend)
    .WithEnvironment("ServiceBus__TopicName", beTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__SubscriptionName", beSub.Resource.SubscriptionName)
    .WithEnvironment("ServiceBus__BroadcastTopicName", broadcastTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__BroadcastSubscriptionName", consumerTwoBroadcastSub.Resource.SubscriptionName)
    .WithEnvironment("ServiceBus__RoutedTopicName", routedTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__EuOrdersSubscriptionName", euOrders.Resource.SubscriptionName)
    .WaitFor(beSub)
    .WaitFor(consumerTwoBroadcastSub)
    .WaitFor(euOrders);

// Discovery only: the Web can start independently of the consumers.
webfrontend.WithReference(consumerOne).WithReference(consumerTwo);

builder.Build().Run();

static AzureServiceBusRule MatchProperty(string name, string property, string value) => new(name)
{
    FilterType = AzureServiceBusFilterType.CorrelationFilter,
    CorrelationFilter = new() { Properties = { [property] = value } }
};
