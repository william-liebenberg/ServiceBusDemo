using Azure.Messaging.ServiceBus;
using ServiceBusDemo.ServiceDefaults;

namespace ServiceBusDemo.ConsumerOne;

public sealed class MessageConsumer(
    ServiceBusClient client,
    IConfiguration configuration,
    ILogger<MessageConsumer> logger,
    DeliveryReportQueue reports)
    : ControlledMessageConsumer(client, configuration, logger, reports, "ConsumerOne", "consumer-one",
        configuration["ServiceBus:HighPrioritySubscriptionName"] ?? "high-priority",
        configuration["ServiceBus:AllRoutedSubscriptionName"] ?? "all");
