using Azure.Messaging.ServiceBus;
using ServiceBusDemo.ServiceDefaults;

namespace ServiceBusDemo.ConsumerTwo;

public sealed class MessageConsumer(
    ServiceBusClient client,
    IConfiguration configuration,
    ILogger<MessageConsumer> logger,
    DeliveryReportQueue reports)
    : ControlledMessageConsumer(client, configuration, logger, reports, "ConsumerTwo", "consumer-two");
