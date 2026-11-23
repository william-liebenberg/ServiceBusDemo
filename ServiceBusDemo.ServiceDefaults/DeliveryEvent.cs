using System.Text.Json.Serialization;

namespace ServiceBusDemo.ServiceDefaults;

public sealed record DeliveryEvent(
    [property: JsonRequired] string MessageId,
    [property: JsonRequired] string Pattern,
    [property: JsonRequired] string Consumer,
    [property: JsonRequired] string Topic,
    [property: JsonRequired] string Subscription,
    [property: JsonRequired] string Body,
    [property: JsonRequired, JsonNumberHandling(JsonNumberHandling.Strict)] int DeliveryCount,
    [property: JsonRequired] DateTimeOffset ReceivedAt);
