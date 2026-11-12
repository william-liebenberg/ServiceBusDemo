var builder = DistributedApplication.CreateBuilder(args);

var asb = builder.AddAzureServiceBus("messaging")
    .RunAsEmulator(emulator => emulator.WithConfiguration(configuration =>
    {
        var namespaceConfiguration = configuration["UserConfig"]?["Namespaces"]?[0]
            ?? throw new InvalidOperationException("The Azure Service Bus emulator configuration must define a namespace.");
        namespaceConfiguration["Name"] = "sbemulatorns";
    }));

// Competing consumers: both consumers share a single subscription, so each message is processed by only one of them.
var beTopic = asb.AddServiceBusTopic("businessEvent", topicName: "business-events");
var beSub = beTopic.AddServiceBusSubscription("business-events");

// Publish/subscribe: each consumer has its own subscription, so every consumer receives a copy of each message.
var broadcastTopic = asb.AddServiceBusTopic("broadcastEvent", topicName: "broadcast-events");
var consumerOneBroadcastSub = broadcastTopic.AddServiceBusSubscription("broadcast-consumer-one", subscriptionName: "consumer-one");
var consumerTwoBroadcastSub = broadcastTopic.AddServiceBusSubscription("broadcast-consumer-two", subscriptionName: "consumer-two");

var producer = builder.AddProject<Projects.ServiceBusDemo_Producer>("producer")
    .WithHttpHealthCheck("/health")
    .WithReference(asb)
    .WithEnvironment("ServiceBus__TopicName", beTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__BroadcastTopicName", broadcastTopic.Resource.TopicName)
    .WaitFor(beTopic)
    .WaitFor(broadcastTopic);

// The web frontend is the producer API's UI; parenting nests it under the producer in the dashboard.
builder.AddProject<Projects.ServiceBusDemo_Web>("webfrontend")
    .WithExternalHttpEndpoints()
    .WithHttpHealthCheck("/health")
    .WithReference(producer)
    .WithParentRelationship(producer)
    .WaitFor(producer);

builder.AddProject<Projects.ServiceBusDemo_ConsumerOne>("consumer-one")
    .WithHttpHealthCheck("/health")
    .WithReference(asb)
    .WithEnvironment("ServiceBus__TopicName", beTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__SubscriptionName", beSub.Resource.SubscriptionName)
    .WithEnvironment("ServiceBus__BroadcastTopicName", broadcastTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__BroadcastSubscriptionName", consumerOneBroadcastSub.Resource.SubscriptionName)
    .WaitFor(beSub)
    .WaitFor(consumerOneBroadcastSub);

builder.AddProject<Projects.ServiceBusDemo_ConsumerTwo>("consumer-two")
    .WithHttpHealthCheck("/health")
    .WithReference(asb)
    .WithEnvironment("ServiceBus__TopicName", beTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__SubscriptionName", beSub.Resource.SubscriptionName)
    .WithEnvironment("ServiceBus__BroadcastTopicName", broadcastTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__BroadcastSubscriptionName", consumerTwoBroadcastSub.Resource.SubscriptionName)
    .WaitFor(beSub)
    .WaitFor(consumerTwoBroadcastSub);

builder.Build().Run();
